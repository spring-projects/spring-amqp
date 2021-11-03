/*
 * Copyright 2002-2021 the original author or authors.
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

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.utils.SerializationUtils;

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
 */
public class SimpleMessageConverter extends AllowedListDeserializingMessageConverter {

	public static final String DEFAULT_CHARSET = "UTF-8";

	private volatile String defaultCharset = DEFAULT_CHARSET;

	/**
	 * Specify the default charset to use when converting to or from text-based
	 * Message body content. If not specified, the charset will be "UTF-8".
	 *
	 * @param defaultCharset The default charset.
	 */
	public void setDefaultCharset(String defaultCharset) {
		this.defaultCharset = (defaultCharset != null) ? defaultCharset : DEFAULT_CHARSET;
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
			if (contentType != null && contentType.startsWith("text")) {
				String encoding = properties.getContentEncoding();
				if (encoding == null) {
					encoding = this.defaultCharset;
				}
				try {
					content = new String(message.getBody(), encoding);
				}
				catch (UnsupportedEncodingException e) {
					throw new MessageConversionException(
							"failed to convert text-based Message content", e);
				}
			}
			else if (contentType != null &&
					contentType.equals(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT)) {
				try {
					content = SerializationUtils.deserialize(
							createObjectInputStream(new ByteArrayInputStream(message.getBody())));
				}
				catch (IOException | IllegalArgumentException | IllegalStateException e) {
					throw new MessageConversionException(
							"failed to convert serialized Message content", e);
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
		if (object instanceof byte[]) {
			bytes = (byte[]) object;
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_BYTES);
		}
		else if (object instanceof String) {
			try {
				bytes = ((String) object).getBytes(this.defaultCharset);
			}
			catch (UnsupportedEncodingException e) {
				throw new MessageConversionException(
						"failed to convert to Message content", e);
			}
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
			messageProperties.setContentEncoding(this.defaultCharset);
		}
		else if (object instanceof Serializable) {
			try {
				bytes = SerializationUtils.serialize(object);
			}
			catch (IllegalArgumentException e) {
				throw new MessageConversionException(
						"failed to convert to serialized Message content", e);
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
	 * Create an ObjectInputStream for the given InputStream and codebase. The default
	 * implementation creates an ObjectInputStream.
	 * @param is the InputStream to read from
	 * @return the new ObjectInputStream instance to use
	 * @throws IOException if creation of the ObjectInputStream failed
	 */
	@SuppressWarnings("deprecation")
	protected ObjectInputStream createObjectInputStream(InputStream is) throws IOException {
		return new ObjectInputStream(is) {

			@Override
			protected Class<?> resolveClass(ObjectStreamClass classDesc) throws IOException, ClassNotFoundException {
				Class<?> clazz = super.resolveClass(classDesc);
				checkAllowedList(clazz);
				return clazz;
			}

		};
	}

}
