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

import java.lang.reflect.Type;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Message converter interface.
 *
 * @author Mark Fisher
 * @author Mark Pollack
 * @author Gary Russell
 */
public interface MessageConverter {

	/**
	 * Convert a Java object to a Message.
	 * @param object the object to convert
	 * @param messageProperties The message properties.
	 * @return the Message
	 * @throws MessageConversionException in case of conversion failure
	 */
	Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException;

	/**
	 * Convert a Java object to a Message.
	 * The default implementation calls {@link #toMessage(Object, MessageProperties)}.
	 * @param object the object to convert
	 * @param messageProperties The message properties.
	 * @param genericType the type to use to populate type headers.
	 * @return the Message
	 * @throws MessageConversionException in case of conversion failure
	 * @since 2.1
	 */
	default Message toMessage(Object object, MessageProperties messageProperties, @Nullable Type genericType)
			throws MessageConversionException {

		return toMessage(object, messageProperties);
	}

	/**
	 * Convert from a Message to a Java object.
	 * @param message the message to convert
	 * @return the converted Java object
	 * @throws MessageConversionException in case of conversion failure
	 */
	Object fromMessage(Message message) throws MessageConversionException;

}
