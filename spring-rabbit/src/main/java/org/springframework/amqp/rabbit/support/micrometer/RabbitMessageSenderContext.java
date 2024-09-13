/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.amqp.rabbit.support.micrometer;

import org.springframework.amqp.core.Message;

import io.micrometer.observation.transport.SenderContext;

/**
 * {@link SenderContext} for {@link Message}s.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class RabbitMessageSenderContext extends SenderContext<Message> {

	private final String beanName;

	private final String destination;

	private final String exchange;

	private final String routingKey;

	public RabbitMessageSenderContext(Message message, String beanName, String exchange, String routingKey) {
		super((carrier, key, value) -> message.getMessageProperties().setHeader(key, value));
		setCarrier(message);
		this.beanName = beanName;
		this.exchange = exchange;
		this.routingKey = routingKey;
		this.destination = exchange + "/" + routingKey;
		setRemoteServiceName("RabbitMQ");
	}

	public String getBeanName() {
		return this.beanName;
	}

	/**
	 * Return the destination - {@code exchange/routingKey}.
	 * @return the destination.
	 */
	public String getDestination() {
		return this.destination;
	}

	/**
	 * Return the exchange.
	 * @return the exchange.
	 */
	public String getExchange() {
		return this.exchange;
	}

	/**
	 * Return the routingKey.
	 * @return the routingKey.
	 */
	public String getRoutingKey() {
		return this.routingKey;
	}

}
