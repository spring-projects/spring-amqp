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
 * Simple container collecting information to describe a queue binding. Takes Queue and Exchange
 * instances as arguments to facilitate wiring using @Bean code based configuration.
 * Used in conjunction with AmqpAdmin.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 * @see AmqpAdmin
 */
public class Binding {

	private String queue;

	private String exchange;

	private String routingKey;

	private Map<String, Object> arguments;


	public Binding(Queue queue, FanoutExchange exchange) {
		this.queue = queue.getName();
		this.exchange = exchange.getName();
		this.routingKey = "";
	}

	public Binding(Queue queue, HeadersExchange exchange, Map<String, Object> arguments) {
		this.queue = queue.getName();
		this.exchange = exchange.getName();
		this.routingKey = "";
		this.arguments = arguments;
	}

	public Binding(Queue queue, DirectExchange exchange, String routingKey) {
		this.queue = queue.getName();
		this.exchange = exchange.getName();
		this.routingKey = routingKey;
	}

	public Binding(Queue queue, TopicExchange exchange, String routingKey) {
		this.queue = queue.getName();
		this.exchange = exchange.getName();
		this.routingKey = routingKey;
	}


	public String getQueue() {
		return this.queue;
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

	public void setArguments(Map<String, Object> arguments) {
		this.arguments = arguments;
	}

}
