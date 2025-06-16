/*
 * Copyright 2024-present the original author or authors.
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

package org.springframework.amqp.core;

import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

/**
 * An {@link AbstractExchange} extension for Consistent Hash exchange type.
 *
 * @author Artem Bilan
 *
 * @since 3.2
 *
 * @see AmqpAdmin
 */
public class ConsistentHashExchange extends AbstractExchange {

	/**
	 * Construct a new durable, non-auto-delete Exchange with the provided name.
	 * @param name the name of the exchange.
	 */
	public ConsistentHashExchange(String name) {
		super(name);
	}

	/**
	 * Construct a new Exchange, given a name, durability flag, auto-delete flag.
	 * @param name the name of the exchange.
	 * @param durable true if we are declaring a durable exchange (the exchange will
	 * survive a server restart)
	 * @param autoDelete true if the server should delete the exchange when it is no
	 * longer in use
	 */
	public ConsistentHashExchange(String name, boolean durable, boolean autoDelete) {
		super(name, durable, autoDelete);
	}

	/**
	 * Construct a new Exchange, given a name, durability flag, and auto-delete flag, and
	 * arguments.
	 * @param name the name of the exchange.
	 * @param durable true if we are declaring a durable exchange (the exchange will
	 * survive a server restart)
	 * @param autoDelete true if the server should delete the exchange when it is no
	 * longer in use
	 * @param arguments the arguments used to declare the exchange
	 */
	public ConsistentHashExchange(String name, boolean durable, boolean autoDelete,
			@Nullable Map<String, @Nullable Object> arguments) {

		super(name, durable, autoDelete, arguments);
		Assert.isTrue(arguments == null ||
						(!(arguments.containsKey("hash-header") && arguments.containsKey("hash-property"))),
				"The 'hash-header' and 'hash-property' are mutually exclusive.");
	}

	/**
	 * Specify a header name from the message to hash.
	 * @param headerName the header name for hashing.
	 */
	public void setHashHeader(String headerName) {
		Map<String, Object> arguments = getArguments();
		Assert.isTrue(!arguments.containsKey("hash-property"),
				"The 'hash-header' and 'hash-property' are mutually exclusive.");
		arguments.put("hash-header", headerName);
	}

	/**
	 * Specify a property name from the message to hash.
	 * @param propertyName the property name for hashing.
	 */
	public void setHashProperty(String propertyName) {
		Map<String, Object> arguments = getArguments();
		Assert.isTrue(!arguments.containsKey("hash-header"),
				"The 'hash-header' and 'hash-property' are mutually exclusive.");
		arguments.put("hash-property", propertyName);
	}

	@Override
	public String getType() {
		return ExchangeTypes.CONSISTENT_HASH;
	}

}
