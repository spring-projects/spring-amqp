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

import java.util.HashMap;
import java.util.Map;


/**
 * Common properties that describe all exchange types.
 * <p>Subclasses of this class are typically used with administrative operations that declare an exchange.
 *
 * @author Mark Pollack
 * @author Gary Russell
 *
 * @see AmqpAdmin
 */
public abstract class AbstractExchange extends AbstractDeclarable implements Exchange {

	private final String name;

	private final boolean durable;

	private final boolean autoDelete;

	private final Map<String, Object> arguments;

	private volatile boolean delayed;

	/**
	 * Construct a new Exchange for bean usage.
	 * @param name the name of the exchange.
	 */
	public AbstractExchange(String name) {
		this(name, true, false);
	}

	/**
	 * Construct a new Exchange, given a name, durability flag, auto-delete flag.
	 * @param name the name of the exchange.
	 * @param durable true if we are declaring a durable exchange (the exchange will survive a server restart)
	 * @param autoDelete true if the server should delete the exchange when it is no longer in use
	 */
	public AbstractExchange(String name, boolean durable, boolean autoDelete) {
		this(name, durable, autoDelete, null);
	}

	/**
	 * Construct a new Exchange, given a name, durability flag, and auto-delete flag, and arguments.
	 * @param name the name of the exchange.
	 * @param durable true if we are declaring a durable exchange (the exchange will survive a server restart)
	 * @param autoDelete true if the server should delete the exchange when it is no longer in use
	 * @param arguments the arguments used to declare the exchange
	 */
	public AbstractExchange(String name, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
		super();
		this.name = name;
		this.durable = durable;
		this.autoDelete = autoDelete;
		if (arguments != null) {
			this.arguments = arguments;
		}
		else {
			this.arguments = new HashMap<String, Object>();
		}
	}

	@Override
	public abstract String getType();

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public boolean isDurable() {
		return this.durable;
	}

	@Override
	public boolean isAutoDelete() {
		return this.autoDelete;
	}

	protected synchronized void addArgument(String argName, Object argValue) {
		this.arguments.put(argName, argValue);
	}
	/**
	 * Return the collection of arbitrary arguments to use when declaring an exchange.
	 * @return the collection of arbitrary arguments to use when declaring an exchange.
	 */
	@Override
	public Map<String, Object> getArguments() {
		return this.arguments;
	}

	@Override
	public boolean isDelayed() {
		return this.delayed;
	}

	public void setDelayed(boolean delayed) {
		this.delayed = delayed;
	}

	@Override
	public String toString() {
		return "Exchange [name=" + this.name +
						 ", type=" + getType() +
						 ", durable=" + this.durable +
						 ", autoDelete=" + this.autoDelete +
						 ", arguments="	+ this.arguments + "]";
	}

}
