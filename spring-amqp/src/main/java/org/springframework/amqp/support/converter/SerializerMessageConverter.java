/*
 * Copyright 2002-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.support.converter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.core.ConfigurableObjectInputStream;
import org.springframework.core.serializer.DefaultDeserializer;
import org.springframework.core.serializer.DefaultSerializer;
import org.springframework.core.serializer.Deserializer;
import org.springframework.core.serializer.Serializer;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

/**
 * Implementation of {@link MessageConverter} that can work with Strings or native objects
 * of any kind via the {@link Serializer} and {@link Deserializer} abstractions in Spring.
 * The {@link #toMessage(Object, MessageProperties)} method simply checks the type of the
 * provided instance while the {@link #fromMessage(Message)} method relies upon the
 * {@link MessageProperties#getContentType() content-type} of the provided Message.
 * <p>
 * If a {@link DefaultDeserializer} is configured (default),
 * the {@link #setAllowedListPatterns(java.util.List) allowed patterns} will be applied
 * (if configured); for all other deserializers, the deserializer is responsible for
 * checking classes, if necessary.
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
public class SerializerMessageConverter extends AllowedListDeserializingMessageConverter
		implements BeanClassLoaderAware {

	public static final String DEFAULT_CHARSET = StandardCharsets.UTF_8.name();

	private String defaultCharset = DEFAULT_CHARSET;

	private Serializer<Object> serializer = new DefaultSerializer();

	private Deserializer<Object> deserializer = new DefaultDeserializer();

	private boolean ignoreContentType = false;

	private ClassLoader defaultDeserializerClassLoader = ClassUtils.getDefaultClassLoader();

	private boolean usingDefaultDeserializer = true;

	/**
	 * Flag to signal that the content type should be ignored and the deserializer used irrespective if it is a text
	 * message. Defaults to false, in which case the default encoding is used to convert a text message to a String.
	 * @param ignoreContentType the flag value to set
	 */
	public void setIgnoreContentType(boolean ignoreContentType) {
		this.ignoreContentType = ignoreContentType;
	}

	/**
	 * Specify the default charset to use when converting to or from text-based Message body content. If not specified,
	 * the charset will be "UTF-8".
	 * @param defaultCharset The default charset.
	 */
	public void setDefaultCharset(@Nullable String defaultCharset) {
		this.defaultCharset = (defaultCharset != null) ? defaultCharset : DEFAULT_CHARSET;
	}

	/**
	 * The serializer to use for converting Java objects to message bodies.
	 * @param serializer the serializer to set
	 */
	public void setSerializer(Serializer<Object> serializer) {
		this.serializer = serializer;
	}

	/**
	 * The deserializer to use for converting from message body to Java object.
	 * @param deserializer the deserializer to set
	 */
	public void setDeserializer(Deserializer<Object> deserializer) {
		this.deserializer = deserializer;
		this.usingDefaultDeserializer = false;
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.defaultDeserializerClassLoader = classLoader;
	}

	/**
	 * Converts from a AMQP Message to an Object.
	 */
	@Override
	public Object fromMessage(Message message) throws MessageConversionException {
		Object content = null;
		MessageProperties properties = message.getMessageProperties();
		if (properties != null) {
			String contentType = properties.getContentType();
			if (contentType != null && contentType.startsWith("text") && !this.ignoreContentType) {
				content = asString(message, properties);
			}
			else if (contentType != null && contentType.equals(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT)
					|| this.ignoreContentType) {
				content = deserialize(message);
			}
		}
		if (content == null) {
			content = message.getBody();
		}
		return content;
	}

	private Object deserialize(Message message) {
		try {
			ByteArrayInputStream inputStream = new ByteArrayInputStream(message.getBody());
			if (this.usingDefaultDeserializer) {
				return deserialize(inputStream);
			}
			else {
				return this.deserializer.deserialize(inputStream);
			}
		}
		catch (IOException e) {
			throw new MessageConversionException("Could not convert message body", e);
		}
	}

	private Object asString(Message message, MessageProperties properties) {
		String encoding = properties.getContentEncoding();
		if (encoding == null) {
			encoding = this.defaultCharset;
		}
		try {
			return new String(message.getBody(), encoding);
		}
		catch (UnsupportedEncodingException e) {
			throw new MessageConversionException("failed to convert text-based Message content", e);
		}
	}

	private Object deserialize(ByteArrayInputStream inputStream) throws IOException {
		try (ObjectInputStream objectInputStream = new ConfigurableObjectInputStream(inputStream,
				this.defaultDeserializerClassLoader) {

			@Override
			protected Class<?> resolveClass(ObjectStreamClass classDesc)
					throws IOException, ClassNotFoundException {
				Class<?> clazz = super.resolveClass(classDesc);
				checkAllowedList(clazz);
				return clazz;
			}

		}) {
			return objectInputStream.readObject();
		}
		catch (ClassNotFoundException ex) {
			throw new IOException("Failed to deserialize object type", ex);
		}
	}

	/**
	 * Creates an AMQP Message from the provided Object.
	 */
	@Override
	protected Message createMessage(Object object, MessageProperties messageProperties)
			throws MessageConversionException {

		byte[] bytes;
		if (object instanceof String string) {
			try {
				bytes = string.getBytes(this.defaultCharset);
			}
			catch (UnsupportedEncodingException e) {
				throw new MessageConversionException("failed to convert Message content", e);
			}
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
			messageProperties.setContentEncoding(this.defaultCharset);
		}
		else if (object instanceof byte[] objectBytes) {
			bytes = objectBytes;
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_BYTES);
		}
		else {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			try {
				this.serializer.serialize(object, output);
			}
			catch (IOException e) {
				throw new MessageConversionException("Cannot convert object to bytes", e);
			}
			bytes = output.toByteArray();
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
		}

		messageProperties.setContentLength(bytes.length);
		return new Message(bytes, messageProperties);
	}

}
