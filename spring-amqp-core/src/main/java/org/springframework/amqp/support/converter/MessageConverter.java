/*
 * Copyright 2002-2010 the original author or authors.
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

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * @author Mark Fisher
 * @author Mark Pollack
 */
public interface MessageConverter {

	/**
	 * Convert a Java object to a Rabbit Message.
	 * @param object the object to convert
	 * @return the Rabbit Message
	 * @throws MessageConversionException in case of conversion failure
	 */
	Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException;

	/**
	 * Convert from a Rabbit GetResponse to a Java object.
	 * @param message the message to convert
	 * @return the converted Java object
	 * @throws MessageConversionException in case of conversion failure
	 */
	Object fromMessage(Message message) throws MessageConversionException;

}
