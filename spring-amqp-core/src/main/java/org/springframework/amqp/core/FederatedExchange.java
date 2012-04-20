/*
 * Copyright 2002-2012 the original author or authors.
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
 * 
 * @see AmqpAdmin
 *
 * @author Gary Russell
 */
public class FederatedExchange extends AbstractExchange {

	public static final FederatedExchange DEFAULT = new FederatedExchange("");

	private static final String BACKING_TYPE_ARG = "type";

	private static final String UPSTREAM_SET_ARG = "upstream-set";

	public FederatedExchange(String name) {
		super(name);
	}

	public FederatedExchange(String name, boolean durable, boolean autoDelete) {
		super(name, durable, autoDelete);
	}

	public FederatedExchange(String name, boolean durable, boolean autoDelete, Map<String,Object> arguments) {
		super(name, durable, autoDelete, arguments);
	}

	public void setBackingType(String backingType) {
		this.addArgument(BACKING_TYPE_ARG, backingType);
	}

	public void setUpstreamSet(String upstreamSet) {
		this.addArgument(UPSTREAM_SET_ARG, upstreamSet);
	}

	@Override
	public final String getType() {
		return ExchangeTypes.FEDERATED;
	}

}
