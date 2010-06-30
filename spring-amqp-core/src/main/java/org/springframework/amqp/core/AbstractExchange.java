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

import java.util.Map;


/**
 * Define common properties for all exchange types.
 * 
 * @author Mark Pollack
 */
public abstract class AbstractExchange {

	protected String name;

	private boolean passive = false;

	private boolean durable = false;

	private boolean autoDelete = false;

	private Map<String, Object> arguments = null;


	public abstract ExchangeType getExchangeType();

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

	public boolean isAutoDelete() {
		return autoDelete;
	}

	public void setAutoDelete(boolean autoDelete) {
		this.autoDelete = autoDelete;
	}

	public Map<String, Object> getArguments() {
		return arguments;
	}

	public void setArguments(Map<String, Object> arguments) {
		this.arguments = arguments;
	}

	@Override
	public String toString() {
		return "Exchange [name=" + name + 
						 ", type=" + getExchangeType().name() +
						 ", passive=" + passive + 
						 ", durable=" + durable +
						 ", autoDelete=" + autoDelete + 
						 ", arguments="	+ arguments + "]";
	}

}
