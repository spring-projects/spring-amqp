/*
 * Copyright 2002-2025 the original author or authors.
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
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.core.ConfigurableObjectInputStream;
import org.springframework.util.ClassUtils;

/**
 * Implementation of {@link MessageConverter} that can work with Strings, Serializable
 * instances, or byte arrays. The {@link #toMessage(Object, MessageProperties)} method
 * simply checks the type of the provided instance while the {@link #fromMessage(Message)}
 * method relies upon the {@link MessageProperties#getContentType() content-type} of the
 * provided Message.
 *
 * @author Mark Fisher
 * @author Oleg Zhurakousky
 * @author Gary Russell
 * @author Artem Bilan
 */
public class SimpleMessageConverter extends AllowedListDeserializingMessageConverter implements BeanClassLoaderAware {

	public static final String DEFAULT_CHARSET = "UTF-8";

	private String defaultCharset = DEFAULT_CHARSET;

	private @Nullable ClassLoader classLoader = ClassUtils.getDefaultClassLoader();

	/**
	 * Specify the default charset to use when converting to or from text-based
	 * Message body content. If not specified, the charset will be "UTF-8".
	 * @param defaultCharset The default charset.
	 */
	public void setDefaultCharset(@Nullable String defaultCharset) {
		this.defaultCharset = (defaultCharset != null) ? defaultCharset : DEFAULT_CHARSET;
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	/**
	 * Converts from a AMQP Message to an Object.
	 */
	@Override
	public Object fromMessage(Message message) throws MessageConversionException {
		Object content = null;
		MessageProperties properties = message.getMessageProperties();
		String contentType = properties.getContentType();
		if (contentType.startsWith("text")) {
			String encoding = properties.getContentEncoding();
			if (encoding == null) {
				encoding = this.defaultCharset;
			}
			try {
				content = new String(message.getBody(), encoding);
			}
			catch (UnsupportedEncodingException e) {
				throw new MessageConversionException("failed to convert text-based Message content", e);
			}
		}
		else if (contentType.equals(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT)) {
			try {
				content = SerializationUtils.deserialize(
						createObjectInputStream(new ByteArrayInputStream(message.getBody())));
			}
			catch (IOException | IllegalArgumentException | IllegalStateException e) {
				throw new MessageConversionException("failed to convert serialized Message content", e);
			}
		}
		if (content == null) {
			content = message.getBody();
		}
		return content;
	}

	/**
	 * Creates an AMQP Message from the provided Object.
	 */
	@Override
	protected Message createMessage(Object object, MessageProperties messageProperties)
			throws MessageConversionException {

		byte[] bytes = null;
		if (object instanceof byte[] objectBytes) {
			bytes = objectBytes;
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_BYTES);
		}
		else if (object instanceof String string) {
			try {
				bytes = string.getBytes(this.defaultCharset);
			}
			catch (UnsupportedEncodingException e) {
				throw new MessageConversionException("failed to convert to Message content", e);
			}
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
			messageProperties.setContentEncoding(this.defaultCharset);
		}
		else if (object instanceof Serializable) {
			try {
				bytes = SerializationUtils.serialize(object);
			}
			catch (IllegalArgumentException e) {
				throw new MessageConversionException("failed to convert to serialized Message content", e);
			}
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
		}
		if (bytes != null) {
			messageProperties.setContentLength(bytes.length);
			return new Message(bytes, messageProperties);
		}
		throw new IllegalArgumentException(getClass().getSimpleName()
				+ " only supports String, byte[] and Serializable payloads, received: " + object.getClass().getName());
	}

	/**
	 * Create an ObjectInputStream for the given InputStream. The default
	 * implementation creates an {@link ConfigurableObjectInputStream} against configured {@link ClassLoader}.
	 * The class for object to deserialize is checked against {@code allowedListPatterns}.
	 * @param is the InputStream to read from
	 * @return the new ObjectInputStream instance to use
	 * @throws IOException if creation of the ObjectInputStream failed
	 */
	protected ObjectInputStream createObjectInputStream(InputStream is) throws IOException {
		return new ConfigurableObjectInputStream(is, this.classLoader) {

			@Override
			protected Class<?> resolveClass(ObjectStreamClass classDesc) throws IOException, ClassNotFoundException {
				Class<?> clazz = super.resolveClass(classDesc);
				checkAllowedList(clazz);
				return clazz;
			}

		};
	}

}
