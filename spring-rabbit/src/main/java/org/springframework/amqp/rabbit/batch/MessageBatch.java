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

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;

/**
 * An object encapsulating a {@link Message} containing the batch of messages,
 * the exchange, and routing key.
 *
 * @param exchange the exchange for batch of messages
 * @param routingKey the routing key for batch
 * @param message the message with a batch
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.4.1
 *
 */
public record MessageBatch(@Nullable String exchange, @Nullable String routingKey, Message message) {

	/**
	 * @return the exchange
	 * @deprecated in favor or {@link #exchange()}.
	 */
	@Deprecated(forRemoval = true, since = "4.0")
	public @Nullable String getExchange() {
		return this.exchange;
	}

	/**
	 * @return the routingKey
	 * @deprecated in favor or {@link #routingKey()}.
	 */
	@Deprecated(forRemoval = true, since = "4.0")
	public @Nullable String getRoutingKey() {
		return this.routingKey;
	}

	/**
	 * @return the message
	 * @deprecated in favor or {@link #message()}.
	 */
	@Deprecated(forRemoval = true, since = "4.0")
	public Message getMessage() {
		return this.message;
	}

}
