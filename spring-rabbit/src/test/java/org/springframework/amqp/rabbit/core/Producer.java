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

package org.springframework.amqp.rabbit.core;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;

public final class Producer {

	private Producer() {
		super();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory("localhost");
		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");

		RabbitTemplate template = new RabbitTemplate();
		template.setConnectionFactory(connectionFactory);
		template.setChannelTransacted(true);
		template.afterPropertiesSet();

		final String routingKey = TestConstants.ROUTING_KEY;
		QueueUtils.declareTestQueue(template, routingKey);

		// send message
		sendMessages(template, TestConstants.EXCHANGE_NAME, routingKey, TestConstants.NUM_MESSAGES);
	}

	private static void sendMessages(RabbitTemplate template,
			final String exchange, final String routingKey, int numMessages) {
		for (int i = 1; i <= numMessages; i++) {
			byte[] bytes = "testing".getBytes();
			MessageProperties properties = new MessageProperties();
			properties.getHeaders().put("float", 3.14f);
			Message message = new Message(bytes, properties);
			template.send(exchange, routingKey, message);
			System .out .println("sending " + i + "...");
		}
	}

}
