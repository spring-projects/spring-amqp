/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.amqp.rabbit.config;

import java.util.Map;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.FactoryBean;

/**
 * @author Dave Syer
 *
 */
public class BindingFactoryBean implements FactoryBean<Binding> {

	private Map<String, Object> arguments;
	private String routingKey = "";
	private String exchange;
	private Queue destinationQueue;
	private Exchange destinationExchange;

	public void setArguments(Map<String, Object> arguments) {
		this.arguments = arguments;
	}

	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public void setDestinationQueue(Queue destinationQueue) {
		this.destinationQueue = destinationQueue;
	}

	public void setDestinationExchange(Exchange destinationExchange) {
		this.destinationExchange = destinationExchange;
	}

	public Binding getObject() throws Exception {
		String destination;
		DestinationType destinationType;
		if (destinationQueue != null) {
			destination = destinationQueue.getName();
			destinationType = DestinationType.QUEUE;
		} else {
			destination = destinationExchange.getName();
			destinationType = DestinationType.EXCHANGE;
		}
		return new Binding(destination, destinationType, exchange, routingKey, arguments);
	}

	public Class<?> getObjectType() {
		return Binding.class;
	}

	public boolean isSingleton() {
		return true;
	}

}
