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

package org.springframework.amqp.rabbit.config;

import java.util.Map;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.FactoryBean;

/**
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public class BindingFactoryBean implements FactoryBean<Binding> {

	private Map<String, Object> arguments;
	private String routingKey = "";
	private String exchange;
	private Queue destinationQueue;
	private Exchange destinationExchange;
	private Boolean shouldDeclare;
	private AmqpAdmin[] adminsThatShouldDeclare;

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

	public void setShouldDeclare(boolean shouldDeclare) {
		this.shouldDeclare = shouldDeclare;
	}

	public void setAdminsThatShouldDeclare(AmqpAdmin... adminsThatShouldDeclare) {
		this.adminsThatShouldDeclare = adminsThatShouldDeclare;
	}

	public Binding getObject() throws Exception {
		String destination;
		DestinationType destinationType;
		if (this.destinationQueue != null) {
			destination = this.destinationQueue.getName();
			destinationType = DestinationType.QUEUE;
		} else {
			destination = this.destinationExchange.getName();
			destinationType = DestinationType.EXCHANGE;
		}
		Binding binding = new Binding(destination, destinationType, this.exchange, this.routingKey, this.arguments);
		if (this.shouldDeclare != null) {
			binding.setShouldDeclare(this.shouldDeclare);
		}
		if (this.adminsThatShouldDeclare != null) {
			binding.setAdminsThatShouldDeclare((Object[]) this.adminsThatShouldDeclare);
		}
		return binding;
	}

	public Class<?> getObjectType() {
		return Binding.class;
	}

	public boolean isSingleton() {
		return true;
	}

}
