/*
 * Copyright 2002-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.io.UnsupportedEncodingException;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.core.serializer.DefaultDeserializer;
import org.springframework.core.serializer.DefaultSerializer;
import org.springframework.core.serializer.Deserializer;
import org.springframework.core.serializer.Serializer;

/**
 * Implementation of {@link MessageConverter} that can work with Strings or native objects of any kind via the
 * {@link Serializer} and {@link Deserializer} abstractions in Spring. The {@link #toMessage(Object, MessageProperties)}
 * method simply checks the type of the provided instance while the {@link #fromMessage(Message)} method relies upon the
 * {@link MessageProperties#getContentType() content-type} of the provided Message.
 *
 * @author Dave Syer
 */
public class SerializerMessageConverter extends AbstractMessageConverter {

	public static final String DEFAULT_CHARSET = "UTF-8";

	private volatile String defaultCharset = DEFAULT_CHARSET;

	private volatile Serializer<Object> serializer = new DefaultSerializer();

	private volatile Deserializer<Object> deserializer = new DefaultDeserializer();

	private volatile boolean ignoreContentType = false;

	/**
	 * Flag to signal that the content type should be ignored and the deserializer used irrespective if it is a text
	 * message. Defaults to false, in which case the default encoding is used to convert a text message to a String.
	 *
	 * @param ignoreContentType the flag value to set
	 */
	public void setIgnoreContentType(boolean ignoreContentType) {
		this.ignoreContentType = ignoreContentType;
	}

	/**
	 * Specify the default charset to use when converting to or from text-based Message body content. If not specified,
	 * the charset will be "UTF-8".
	 *
	 * @param defaultCharset The default charset.
	 */
	public void setDefaultCharset(String defaultCharset) {
		this.defaultCharset = (defaultCharset != null) ? defaultCharset : DEFAULT_CHARSET;
	}

	/**
	 * The serializer to use for converting Java objects to message bodies.
	 *
	 * @param serializer the serializer to set
	 */
	public void setSerializer(Serializer<Object> serializer) {
		this.serializer = serializer;
	}

	/**
	 * The deserializer to use for converting from message body to Java object.
	 *
	 * @param deserializer the deserializer to set
	 */
	public void setDeserializer(Deserializer<Object> deserializer) {
		this.deserializer = deserializer;
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
				String encoding = properties.getContentEncoding();
				if (encoding == null) {
					encoding = this.defaultCharset;
				}
				try {
					content = new String(message.getBody(), encoding);
				} catch (UnsupportedEncodingException e) {
					throw new MessageConversionException("failed to convert text-based Message content", e);
				}
			} else if (contentType != null && contentType.equals(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT)
					|| this.ignoreContentType) {
				try {
					content = this.deserializer.deserialize(new ByteArrayInputStream(message.getBody()));
				} catch (IOException e) {
					throw new MessageConversionException("Could not convert message body", e);
				}
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
	protected Message createMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
		byte[] bytes = null;
		if (object instanceof String) {
			try {
				bytes = ((String) object).getBytes(this.defaultCharset);
			} catch (UnsupportedEncodingException e) {
				throw new MessageConversionException("failed to convert Message content", e);
			}
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
			messageProperties.setContentEncoding(this.defaultCharset);
		} else if (object instanceof byte[]) {
			bytes = (byte[]) object;
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_BYTES);
		} else {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			try {
				this.serializer.serialize(object, output);
			} catch (IOException e) {
				throw new MessageConversionException("Cannot convert object to bytes", e);
			}
			bytes = output.toByteArray();
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
		}
		if (bytes != null) {
			messageProperties.setContentLength(bytes.length);
		}
		return new Message(bytes, messageProperties);
	}

}
