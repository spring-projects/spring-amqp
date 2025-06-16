/*
 * Copyright 2014-present the original author or authors.
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

package org.springframework.amqp.rabbit.batch;

import org.springframework.amqp.core.Message;

/**
 * An object encapsulating a {@link Message} containing the batch of messages,
 * the exchange, and routing key.
 *
 * @author Gary Russell
 * @since 1.4.1
 *
 */
public class MessageBatch {

	private final String exchange;

	private final String routingKey;

	private final Message message;

	public MessageBatch(String exchange, String routingKey, Message message) {
		this.exchange = exchange;
		this.routingKey = routingKey;
		this.message = message;
	}

	/**
	 * @return the exchange
	 */
	public String getExchange() {
		return this.exchange;
	}

	/**
	 * @return the routingKey
	 */
	public String getRoutingKey() {
		return this.routingKey;
	}

	/**
	 * @return the message
	 */
	public Message getMessage() {
		return this.message;
	}

}
