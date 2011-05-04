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

package org.springframework.amqp.core;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.support.converter.MessageConverter;

/**
 * Specifies a basic set of AMQP operations.
 * 
 * Provides synchronous send and receive methods.  The {@link #convertAndSend(Object)} and {@link #receiveAndConvert()}
 * methods allow let you send and receive POJO objects.  Implementations are expected to
 * delegate to an instance of {@link MessageConverter}
 * to perform conversion to and from AMQP byte[] payload type.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
public interface AmqpTemplate {

	// send methods for messages

	void send(Message message) throws AmqpException;

	void send(String routingKey, Message message) throws AmqpException;

	void send(String exchange, String routingKey, Message message) throws AmqpException;

	// send methods with conversion

	void convertAndSend(Object message) throws AmqpException;

	void convertAndSend(String routingKey, Object message) throws AmqpException;

	void convertAndSend(String exchange, String routingKey, Object message) throws AmqpException;

	void convertAndSend(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException;

	void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor) throws AmqpException;

	void convertAndSend(String exchange, String routingKey, Object message, MessagePostProcessor messagePostProcessor) throws AmqpException;

	// receive methods for messages

	Message receive() throws AmqpException;

	Message receive(String queueName) throws AmqpException;

	// receive methods with conversion

	Object receiveAndConvert() throws AmqpException;

	Object receiveAndConvert(String queueName) throws AmqpException;

	// send and receive methods for messages

	Message sendAndReceive(Message message) throws AmqpException;

	Message sendAndReceive(String routingKey, Message message) throws AmqpException;

	Message sendAndReceive(String exchange, String routingKey, Message message) throws AmqpException;

	// send and receive methods with conversion

	Object convertSendAndReceive(Object message) throws AmqpException;

	Object convertSendAndReceive(String routingKey, Object message) throws AmqpException;

	Object convertSendAndReceive(String exchange, String routingKey, Object message) throws AmqpException;

}
