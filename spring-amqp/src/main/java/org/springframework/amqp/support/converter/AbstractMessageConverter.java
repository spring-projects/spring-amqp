/*
 * Copyright 2002-present the original author or authors.
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
import java.util.UUID;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Convenient base class for {@link MessageConverter} implementations.
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public abstract class AbstractMessageConverter implements MessageConverter {

	private boolean createMessageIds = false;

	/**
	 * Flag to indicate that new messages should have unique identifiers added to their properties before sending.
	 * Default false.
	 * @param createMessageIds the flag value to set
	 */
	public void setCreateMessageIds(boolean createMessageIds) {
		this.createMessageIds = createMessageIds;
	}

	/**
	 * Flag to indicate that new messages should have unique identifiers added to their properties before sending.
	 * @return the flag value
	 */
	protected boolean isCreateMessageIds() {
		return this.createMessageIds;
	}

	@Override
	public final Message toMessage(Object object, MessageProperties messageProperties)
			throws MessageConversionException {

		return toMessage(object, messageProperties, null);
	}

	@Override
	public final Message toMessage(Object object, @Nullable MessageProperties messagePropertiesArg,
			@Nullable Type genericType)
			throws MessageConversionException {

		MessageProperties messageProperties = messagePropertiesArg;
		if (messageProperties == null) {
			messageProperties = new MessageProperties();
		}
		Message message = createMessage(object, messageProperties, genericType);
		messageProperties = message.getMessageProperties();
		if (this.createMessageIds && messageProperties.getMessageId() == null) {
			messageProperties.setMessageId(UUID.randomUUID().toString());
		}
		return message;
	}

	/**
	 * Crate a message from the payload object and message properties provided. The message id will be added to the
	 * properties if necessary later.
	 * @param object the payload
	 * @param messageProperties the message properties (headers)
	 * @param genericType the type to convert from - used to populate type headers.
	 * @return a message
	 * @since 2.1
	 */
	protected Message createMessage(Object object, MessageProperties messageProperties, @Nullable Type genericType) {
		return createMessage(object, messageProperties);
	}

	/**
	 * Crate a message from the payload object and message properties provided. The message id will be added to the
	 * properties if necessary later.
	 * @param object the payload.
	 * @param messageProperties the message properties (headers).
	 * @return a message.
	 */
	protected abstract Message createMessage(Object object, MessageProperties messageProperties);

}
