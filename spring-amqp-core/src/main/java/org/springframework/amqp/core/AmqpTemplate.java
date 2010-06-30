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

/**
 * Specifies a basic set of AMQP operations
 * 
 * Implemented by {@link RabbitTemplate}.  Not often used but a useful option to
 * enhance testability, as it can easily be mocked or stubbed.
 * 
 * Provides synchronous send an receive methods.  The convertAndSend and receiveAndConvert
 * methods allow let you send and receive POJO objects.  RabbitTemplate delegates to
 * an instance of {@link org.springframework.support.converter.MessageConverter} to perform
 * conversion to and from Rabbit's byte[] payload type.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
public interface AmqpTemplate {

	void send(MessageCreator messageCreator) throws AmqpException;

	void send(String routingKey, MessageCreator messageCreator) throws AmqpException;
	
	void send(String exchange, String routingKey, MessageCreator messageCreator) throws AmqpException;
		
	void convertAndSend(Object message) throws AmqpException;

	void convertAndSend(String routingKey, Object message) throws AmqpException;
	
	void convertAndSend(String exchange, String routingKey, Object message) throws AmqpException;
	
	void convertAndSend(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException;

	void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor) throws AmqpException;

	void convertAndSend(String exchange, String routingKey, Object message, MessagePostProcessor messagePostProcessor) throws AmqpException;
	
	Message receive() throws AmqpException;
	
	Message receive(String queueName) throws AmqpException;
	
	Object receiveAndConvert() throws AmqpException;
	
	Object receiveAndConvert(String queueName) throws AmqpException;

}
