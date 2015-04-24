/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.amqp.rabbit.core;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;

import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.domain.BindingInfo;
import com.rabbitmq.http.client.domain.ExchangeInfo;
import com.rabbitmq.http.client.domain.QueueInfo;

/**
 * A convenience wrapper for the RabbitMQ {@link Client} providing convenient access to
 * the REST methods using the familiar Spring AMQP domain objects for {@link Queue},
 * {@link Exchange} and {@link Binding}. For more complete access, including access to
 * properties not available in the Spring AMQP domain classes, use the {@link Client}
 * directly.
 *
 * @author Gary Russell
 * @since 1.5
 *
 */
public class RabbitManagementTemplate {

	private static final String DEFAULT_VHOST = "/";

	private final Client rabbitClient;


	/**
	 * Construct a template using uri "localhost:15672/api/" and user guest/guest.
	 */
	public RabbitManagementTemplate() {
		this("http://localhost:15672/api/", "guest", "guest");
	}

	/**
	 * Construct a template using the supplied client.
	 * @param rabbitClient the client.
	 */
	public RabbitManagementTemplate(Client rabbitClient) {
		this.rabbitClient = rabbitClient;
	}

	/**
	 * Construct a template using the supplied uri.
	 * @param uri the uri.
	 * @param username the user.
	 * @param password the password.
	 * @throws URISyntaxException syntax exception.
	 * @throws MalformedURLException  badly formed URL exception.
	 */
	public RabbitManagementTemplate(String uri, String username, String password) {
		try {
			this.rabbitClient = new Client(uri, username, password);
		}
		catch (Exception e) {
			throw new AmqpException(e);
		}
	}

	/**
	 * @return the rabbitClient
	 */
	public Client getClient() {
		return this.rabbitClient;
	}

	/**
	 * Add an exchange to the default vhost ('/').
	 * @param exchange the exchange.
	 */
	public void addExchange(Exchange exchange) {
		addExchange(DEFAULT_VHOST, exchange);
	}

	/**
	 * Add an exchange to the specified vhost.
	 * @param vhost the vhost.
	 * @param exchange the exchange.
	 */
	public void addExchange(String vhost, Exchange exchange) {
		ExchangeInfo info = new ExchangeInfo();
		info.setArguments(exchange.getArguments());
		info.setAutoDelete(exchange.isAutoDelete());
		info.setDurable(exchange.isDurable());
		info.setType(exchange.getType());
		this.rabbitClient.declareExchange(vhost, exchange.getName(), info);
	}

	/**
	 * Purge a queue in the default vhost ('/').
	 * @param queue the queue.
	 */
	public void purgeQueue(Queue queue) {
		this.rabbitClient.purgeQueue(DEFAULT_VHOST, queue.getName());
	}

	/**
	 * Purge a queue in the provided vhost.
	 * @param vhost the vhost.
	 * @param queue the queue.
	 */
	public void purgeQueue(String vhost, Queue queue) {
		this.rabbitClient.purgeQueue(vhost, queue.getName());
	}

	/**
	 * Delete a queue from the default vhost ('/').
	 * @param queue the queue.
	 */
	public void deleteQueue(Queue queue) {
		this.rabbitClient.deleteQueue(DEFAULT_VHOST, queue.getName());
	}

	/**
	 * Delete a queue from the provided vhost.
	 * @param vhost the vhost.
	 * @param queue the queue.
	 */
	public void deleteQueue(String vhost, Queue queue) {
		this.rabbitClient.deleteQueue(vhost, queue.getName());
	}

	/**
	 * Get a specific queue from the default vhost ('/').
	 * @param name the queue name.
	 * @return the Queue.
	 */
	public Queue getQueue(String name) {
		return getQueue(DEFAULT_VHOST, name);
	}

	/**
	 * Get a specific queue from the provided vhost.
	 * @param name the queue name.
	 * @return the Queue.
	 */
	public Queue getQueue(String vhost, String name) {
		return convert(this.rabbitClient.getQueue(vhost, name));
	}

	/**
	 * Get all queues.
	 * @return the queues.
	 */
	public List<Queue> getQueues() {
		return converteQueueList(this.rabbitClient.getQueues());
	}

	/**
	 * Get all queues in the provided vhost.
	 * @param vhost the vhost.
	 * @return the queues.
	 */
	public List<Queue> getQueues(String vhost) {
		return converteQueueList(this.rabbitClient.getQueues(vhost));
	}

	/**
	 * Add a queue to the default vhost ('/').
	 * @param queue the queue.
	 */
	public void addQueue(Queue queue) {
		addQueue(DEFAULT_VHOST, queue);
	}

	/**
	 * Add a queue to the specified vhost.
	 * @param vhost the vhost.
	 * @param queue the queue.
	 */
	public void addQueue(String vhost, Queue queue) {
		QueueInfo info = new QueueInfo();
		info.setArguments(queue.getArguments());
		info.setAutoDelete(queue.isAutoDelete());
		info.setDurable(queue.isDurable());
		info.setExclusive(queue.isExclusive());
		this.rabbitClient.declareQueue(vhost, queue.getName(), info);
	}

	/**
	 * Delete an exchange from the default vhost ('/').
	 * @param exchange the queue.
	 */
	public void deleteExchange(Exchange exchange) {
		this.rabbitClient.deleteQueue(DEFAULT_VHOST, exchange.getName());
	}

	/**
	 * Delete an exchange from the provided vhost.
	 * @param vhost the vhost.
	 * @param exchange the queue.
	 */
	public void deleteExchange(String vhost, Exchange exchange) {
		this.rabbitClient.deleteExchange(vhost, exchange.getName());
	}

	/**
	 * Get a specific queue from the default vhost ('/').
	 * @param name the exchange name.
	 * @return the Exchange.
	 */
	public Exchange getExchange(String name) {
		return getExchange(DEFAULT_VHOST, name);
	}

	/**
	 * Get a specific exchange from the provided vhost.
	 * @param name the exchange name.
	 * @return the Exchange.
	 */
	public Exchange getExchange(String vhost, String name) {
		return convert(this.rabbitClient.getExchange(vhost, name));
	}

	/**
	 * Get all exchanges.
	 * @return the exchanges.
	 */
	public List<Exchange> getExchanges() {
		return converteExchangeList(this.rabbitClient.getExchanges());
	}

	/**
	 * Get all exchanges in the provided vhost. Only {@link DirectExchange},
	 * {@link FanoutExchange}, {@link HeadersExchange} and {@link TopicExchange}s
	 * are returned.
	 * @param vhost the vhost.
	 * @return the exchanges.
	 */
	public List<Exchange> getExchanges(String vhost) {
		return converteExchangeList(this.rabbitClient.getExchanges(vhost));
	}

	/**
	 * Get all bindings.
	 * @return the bindings.
	 */
	public List<Binding> getBindings() {
		return converteBindingList(this.rabbitClient.getBindings());
	}

	/**
	 * Get all bindings in the provided vhost.
	 * @param vhost the vhost.
	 * @return the bindings.
	 */
	public List<Binding> getBindings(String vhost) {
		return converteBindingList(this.rabbitClient.getBindings(vhost));
	}

	/**
	 * Get all bindings from the provided exchange in the provided vhost.
	 * @param vhost the vhost.
	 * @param exchange the exchange name.
	 * @return the bindings.
	 */
	public List<Binding> getBindingsForExchange(String vhost, String exchange) {
		return converteBindingList(this.rabbitClient.getBindingsBySource(vhost, exchange));
	}

	private List<Queue> converteQueueList(List<QueueInfo> queues) {
		List<Queue> convertedQueues = new ArrayList<Queue>();
		for (QueueInfo qi : queues) {
			convertedQueues.add(convert(qi));
		}
		return convertedQueues;
	}

	private Queue convert(QueueInfo qi) {
		return new Queue(qi.getName(), qi.isDurable(), qi.isExclusive(), qi.isAutoDelete(),
				qi.getArguments());
	}

	private List<Exchange> converteExchangeList(List<ExchangeInfo> exchanges) {
		List<Exchange> convertedExchanges = new ArrayList<Exchange>();
		for (ExchangeInfo ei : exchanges) {
			Exchange converted = convert(ei);
			if (converted != null) {
				convertedExchanges.add(converted);
			}
		}
		return convertedExchanges;
	}

	private Exchange convert(ExchangeInfo ei) {
		if (ei.getType().equals("direct")) {
			return new DirectExchange(ei.getName(), ei.isDurable(), ei.isAutoDelete(), ei.getArguments());
		}
		else if (ei.getType().equals("fanout")) {
			return new FanoutExchange(ei.getName(), ei.isDurable(), ei.isAutoDelete(), ei.getArguments());
		}
		else if (ei.getType().equals("headers")) {
			return new HeadersExchange(ei.getName(), ei.isDurable(), ei.isAutoDelete(), ei.getArguments());
		}
		else if (ei.getType().equals("topic")) {
			return new TopicExchange(ei.getName(), ei.isDurable(), ei.isAutoDelete(), ei.getArguments());
		}
		else {
			return null;
		}

	}

	private List<Binding> converteBindingList(List<BindingInfo> bindings) {
		List<Binding> convertedBindings = new ArrayList<Binding>();
		for (BindingInfo bi : bindings) {
			convertedBindings.add(convert(bi));
		}
		return convertedBindings;
	}

	private Binding convert(BindingInfo bi) {
		return new Binding(bi.getDestination(), DestinationType.valueOf(bi.getDestinationType().toUpperCase()),
				bi.getSource(), bi.getRoutingKey(), bi.getArguments());
	}

}
