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
 * class as arguments to facilitate wiring using @Bean code based configuration.
 * Used in conjunction with RabbitAdminTemplate.
 * 
 * @author Mark Pollack
 * @see RabbitAdminTemplate
 */
public class Binding {

	private String queue;

	private String exchange;

	private String routingKey;

	Map<java.lang.String,java.lang.Object> arguments;


	//Is this really worth the syntax sugar?
	public static Binding binding(Queue queue, Exchange exchange, String routingKey) {
		return new Binding(queue, exchange, routingKey);		
	}

	//TODO would be good to remove this because invalid bindings can be specified (fanout with routing key)
	Binding(Queue queue, Exchange exchange, String routingKey) {
		this.queue = queue.getName();
		this.exchange = exchange.getName();
		this.routingKey = routingKey;
	}

	public Binding(Queue queue, FanoutExchange exchange) {
		this.queue = queue.getName();
		this.exchange = exchange.getName();
		this.routingKey = "";
	}
	
	public Binding(Queue queue, DirectExchange exchange, String routingKey) {
		this.queue = queue.getName();
		this.exchange = exchange.getName();
		this.routingKey = routingKey;
	}
	
	public Binding(Queue queue, DirectExchange exchange) {
		this.queue = queue.getName();
		this.exchange = exchange.getName();
		this.routingKey = "";
	}
	

	public Binding(Queue queue, TopicExchange exchange, String routingKey) {
		this.queue = queue.getName();
		this.exchange = exchange.getName();
		this.routingKey = routingKey;
	}
	
	public Binding(Queue queue, TopicExchange exchange) {
		this.queue = queue.getName();
		this.exchange = exchange.getName();
		this.routingKey = "";
	}
	
	public String getQueue() {
		return queue;
	}

	public String getExchange() {
		return exchange;
	}

	public String getRoutingKey() {
		return routingKey;
	}

	public Map<java.lang.String, java.lang.Object> getArguments() {
		return arguments;
	}

	public void setArguments(Map<java.lang.String, java.lang.Object> arguments) {
		this.arguments = arguments;
	}

}
