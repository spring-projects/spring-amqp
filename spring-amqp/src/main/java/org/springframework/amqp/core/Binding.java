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

package org.springframework.amqp.core;

import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

/**
 * Simple container collecting information to describe a binding. Takes String destination and exchange names as
 * arguments to facilitate wiring using code based configuration. Can be used in conjunction with {@link AmqpAdmin}, or
 * created via a {@link BindingBuilder}.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Artem Bilan
 *
 * @see AmqpAdmin
 */
public class Binding extends AbstractDeclarable {

	/**
	 * The binding destination.
	 */
	public enum DestinationType {

		/**
		 * Queue destination.
		 */
		QUEUE,

		/**
		 * Exchange destination.
		 */
		EXCHANGE;
	}

	private final @Nullable String destination;

	private final @Nullable String exchange;

	private final @Nullable String routingKey;

	private final DestinationType destinationType;

	private final @Nullable Queue lazyQueue;

	public Binding(String destination, DestinationType destinationType, @Nullable String exchange, String routingKey,
			@Nullable Map<String, @Nullable Object> arguments) {

		this(null, destination, destinationType, exchange, routingKey, arguments);
	}

	public Binding(@Nullable Queue lazyQueue, @Nullable String destination, DestinationType destinationType,
			@Nullable String exchange, @Nullable String routingKey, @Nullable Map<String, @Nullable Object> arguments) {

		super(arguments);
		Assert.isTrue(lazyQueue == null || destinationType == DestinationType.QUEUE,
				"'lazyQueue' must be null for destination type " + destinationType);
		Assert.isTrue(lazyQueue != null || destination != null, "`destination` cannot be null");
		this.lazyQueue = lazyQueue;
		this.destination = destination;
		this.destinationType = destinationType;
		this.exchange = exchange;
		this.routingKey = routingKey;
	}

	public @Nullable String getDestination() {
		if (this.lazyQueue != null) {
			return this.lazyQueue.getActualName();
		}
		else {
			return this.destination;
		}
	}

	public DestinationType getDestinationType() {
		return this.destinationType;
	}

	public @Nullable String getExchange() {
		return this.exchange;
	}

	public @Nullable String getRoutingKey() {
		if (this.routingKey == null && this.lazyQueue != null) {
			return this.lazyQueue.getActualName();
		}
		return this.routingKey;
	}

	public boolean isDestinationQueue() {
		return DestinationType.QUEUE.equals(this.destinationType);
	}

	@Override
	public String toString() {
		return "Binding [destination=" + this.destination + ", exchange=" + this.exchange + ", routingKey="
					+ this.routingKey + ", arguments=" + getArguments() + "]";
	}

}
