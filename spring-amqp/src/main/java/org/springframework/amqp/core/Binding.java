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

package org.springframework.amqp.core;

import java.util.Map;

/**
 * Simple container collecting information to describe a binding. Takes String destination and exchange names as
 * arguments to facilitate wiring using code based configuration. Can be used in conjunction with {@link AmqpAdmin}, or
 * created via a {@link BindingBuilder}.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 *
 * @see AmqpAdmin
 */
public class Binding extends AbstractDeclarable {

	public enum DestinationType {
		QUEUE, EXCHANGE;
	}

	private final String destination;

	private final String exchange;

	private final String routingKey;

	private final Map<String, Object> arguments;

	private final DestinationType destinationType;

	public Binding(String destination, DestinationType destinationType, String exchange, String routingKey,
			Map<String, Object> arguments) {
		this.destination = destination;
		this.destinationType = destinationType;
		this.exchange = exchange;
		this.routingKey = routingKey;
		this.arguments = arguments;
	}

	public String getDestination() {
		return this.destination;
	}

	public DestinationType getDestinationType() {
		return this.destinationType;
	}

	public String getExchange() {
		return this.exchange;
	}

	public String getRoutingKey() {
		return this.routingKey;
	}

	public Map<String, Object> getArguments() {
		return this.arguments;
	}

	public boolean isDestinationQueue() {
		return DestinationType.QUEUE.equals(this.destinationType);
	}

	@Override
	public String toString() {
		return "Binding [destination=" + this.destination + ", exchange=" + this.exchange + ", routingKey="
					+ this.routingKey + "]";
	}

}
