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

/**
 * Simple container collecting information to describe a fanout exchange.
 * Used in conjunction with administrative operations.
 * @author Mark Pollack
 * @author Dave Syer
 * @see AmqpAdmin
 */
public class FanoutExchange extends AbstractExchange {

	public FanoutExchange(String name) {
		super(name);
	}

	public FanoutExchange(String name, boolean durable, boolean autoDelete) {
		super(name, durable, autoDelete);
	}

	public FanoutExchange(String name, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
		super(name, durable, autoDelete, arguments);
	}

	@Override
	public final String getType() {
		return ExchangeTypes.FANOUT;
	}

}
