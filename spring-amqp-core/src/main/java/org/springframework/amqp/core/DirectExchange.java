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
 * Simple container collecting information to describe a direct exchange.
 * Used in conjunction with administrative operations.
 * 
 * @see AmqpAdmin
 * 
 * @author Mark Pollack
 */
public class DirectExchange extends AbstractExchange {

	public static final DirectExchange DEFAULT = new DirectExchange("");

	
	public DirectExchange(String name) {
		super(name);
	}

	public DirectExchange(String name, boolean durable, boolean autoDelete) {
		super(name, durable, autoDelete);
	}

	public ExchangeType getExchangeType() {
		return ExchangeType.direct;
	}
}
