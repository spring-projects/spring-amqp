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

/**
 * Simple container collecting information to describe a queue.
 * Used in conjunction with AmqpAdmin.
 * 
 * @author Mark Pollack
 * @see AmqpAdmin
 */
public class Queue  {

	private final String name;

	private volatile boolean durable;

	private volatile boolean exclusive;

	private volatile boolean autoDelete;

	private volatile java.util.Map<java.lang.String,java.lang.Object> arguments;


	public Queue(String name) {
		this.name = name;
	}

	public String getName() {
		return this.name;
	}

	public boolean isDurable() {
		return this.durable;
	}

	public void setDurable(boolean durable) {
		this.durable = durable;
	}

	public boolean isExclusive() {
		return this.exclusive;
	}

	public void setExclusive(boolean exclusive) {
		this.exclusive = exclusive;
	}

	public boolean isAutoDelete() {
		return this.autoDelete;
	}

	public void setAutoDelete(boolean autoDelete) {
		this.autoDelete = autoDelete;
	}

	public java.util.Map<java.lang.String, java.lang.Object> getArguments() {
		return this.arguments;
	}

	public void setArguments(java.util.Map<java.lang.String, java.lang.Object> arguments) {
		this.arguments = arguments;
	}
	

	@Override
	public String toString() {
		return "Queue [name=" + name + ", durable=" + durable + ", autoDelete="
				+ autoDelete + ", exclusive=" + exclusive + ", arguments="
				+ arguments + "]";
	}

}
