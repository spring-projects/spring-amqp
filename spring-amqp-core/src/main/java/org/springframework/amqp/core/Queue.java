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
 * Used in conjunction with RabbitAdminTemplate.
 * 
 * @author Mark Pollack
 * @see RabbitAdminTemplate
 */
public class Queue  {

	private String name;
	
	private boolean passive;
	
	private boolean durable;
	
	private boolean exclusive;
	
	private boolean autoDelete;
	
	private java.util.Map<java.lang.String,java.lang.Object> arguments;

	public Queue(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public boolean isPassive() {
		return passive;
	}

	public void setPassive(boolean passive) {
		this.passive = passive;
	}

	public boolean isDurable() {
		return durable;
	}

	public void setDurable(boolean durable) {
		this.durable = durable;
	}

	public boolean isExclusive() {
		return exclusive;
	}

	public void setExclusive(boolean exclusive) {
		this.exclusive = exclusive;
	}

	public boolean isAutoDelete() {
		return autoDelete;
	}

	public void setAutoDelete(boolean autoDelete) {
		this.autoDelete = autoDelete;
	}

	public java.util.Map<java.lang.String, java.lang.Object> getArguments() {
		return arguments;
	}

	public void setArguments(
			java.util.Map<java.lang.String, java.lang.Object> arguments) {
		this.arguments = arguments;
	}
	
	
	
}
