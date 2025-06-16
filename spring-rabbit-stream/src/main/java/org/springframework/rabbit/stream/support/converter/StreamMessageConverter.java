/*
 * Copyright 2021-present the original author or authors.
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

package org.springframework.rabbit.stream.support.converter;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.rabbit.stream.support.StreamMessageProperties;

/**
 * Converts between {@link com.rabbitmq.stream.Message} and
 * {@link org.springframework.amqp.core.Message}.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public interface StreamMessageConverter extends MessageConverter {

	Message toMessage(Object object, StreamMessageProperties messageProperties) throws MessageConversionException;

	@Override
	default Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
		throw new UnsupportedOperationException();
	}

	@Override
	com.rabbitmq.stream.Message fromMessage(Message message) throws MessageConversionException;

}
