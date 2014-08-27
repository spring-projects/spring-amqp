/*
 * Copyright 2002-2014 the original author or authors.
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

package org.springframework.amqp.rabbit.test;

import java.io.UnsupportedEncodingException;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

/**
 * {@link org.springframework.amqp.core.Message} related utilities.
 *
 * @author Stephane Nicoll
 * @since 1.4
 */
public abstract class MessageTestUtils {

	/**
	 * Create a text message with the specified {@link MessageProperties}. The
	 * content type is set no matter
	 */
	public static Message createTextMessage(String body, MessageProperties properties) {
		properties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		return new org.springframework.amqp.core.Message(toBytes(body), properties);
	}

	/**
	 * Create a text message with the relevant content type.
	 */
	public static Message createTextMessage(String body) {
		return createTextMessage(body, new MessageProperties());
	}


	/**
	 * Extract the text from the specified message.
	 */
	public static String extractText(Message message) {
		try {
			return new String(message.getBody(), SimpleMessageConverter.DEFAULT_CHARSET);
		}
		catch (UnsupportedEncodingException e) {
			throw new IllegalStateException("Should not happen", e);
		}
	}

	private static byte[] toBytes(String content) {
		try {
			return content.getBytes(SimpleMessageConverter.DEFAULT_CHARSET);
		}
		catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		}
	}

}
