/*
 * Copyright 2015-2017 the original author or authors.
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

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AbstractExchange;
import org.springframework.amqp.core.AmqpManagementOperations;
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
 * @author Artem Bilan
 *
 * @since 1.5
 *
 */
public class RabbitManagementTemplate implements AmqpManagementOperations {

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
	 * @param uri the uri - must include user info, e.g.
	 * "http://guest:guest@localhost:15672/api/".
	 * @since 2.0
	 */
	public RabbitManagementTemplate(String uri) {
		try {
			this.rabbitClient = new Client(uri);
		}
		catch (Exception e) {
			throw new AmqpException(e);
		}
	}

	/**
	 * Construct a template using the supplied uri.
	 * @param uri the uri.
	 * @param username the user.
	 * @param password the password.
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

	@Override
	public void addExchange(Exchange exchange) {
		addExchange(DEFAULT_VHOST, exchange);
	}

	@Override
	public void addExchange(String vhost, Exchange exchange) {
		ExchangeInfo info = new ExchangeInfo();
		info.setArguments(exchange.getArguments());
		info.setAutoDelete(exchange.isAutoDelete());
		info.setDurable(exchange.isDurable());
		info.setType(exchange.getType());
		this.rabbitClient.declareExchange(vhost, exchange.getName(), info);
	}

	@Override
	public void purgeQueue(Queue queue) {
		this.rabbitClient.purgeQueue(DEFAULT_VHOST, queue.getName());
	}

	@Override
	public void purgeQueue(String vhost, Queue queue) {
		this.rabbitClient.purgeQueue(vhost, queue.getName());
	}

	@Override
	public void deleteQueue(Queue queue) {
		this.rabbitClient.deleteQueue(DEFAULT_VHOST, queue.getName());
	}

	@Override
	public void deleteQueue(String vhost, Queue queue) {
		this.rabbitClient.deleteQueue(vhost, queue.getName());
	}

	@Override
	public Queue getQueue(String name) {
		return getQueue(DEFAULT_VHOST, name);
	}

	@Override
	public Queue getQueue(String vhost, String name) {
		return convert(this.rabbitClient.getQueue(vhost, name));
	}

	@Override
	public List<Queue> getQueues() {
		return convertQueueList(this.rabbitClient.getQueues());
	}

	@Override
	public List<Queue> getQueues(String vhost) {
		return convertQueueList(this.rabbitClient.getQueues(vhost));
	}

	@Override
	public void addQueue(Queue queue) {
		addQueue(DEFAULT_VHOST, queue);
	}

	@Override
	public void addQueue(String vhost, Queue queue) {
		QueueInfo info = new QueueInfo();
		info.setArguments(queue.getArguments());
		info.setAutoDelete(queue.isAutoDelete());
		info.setDurable(queue.isDurable());
		info.setExclusive(queue.isExclusive());
		this.rabbitClient.declareQueue(vhost, queue.getName(), info);
	}

	@Override
	public void deleteExchange(Exchange exchange) {
		deleteExchange(DEFAULT_VHOST, exchange);
	}

	@Override
	public void deleteExchange(String vhost, Exchange exchange) {
		this.rabbitClient.deleteExchange(vhost, exchange.getName());
	}

	@Override
	public Exchange getExchange(String name) {
		return getExchange(DEFAULT_VHOST, name);
	}

	@Override
	public Exchange getExchange(String vhost, String name) {
		return convert(this.rabbitClient.getExchange(vhost, name));
	}

	@Override
	public List<Exchange> getExchanges() {
		return convertExchangeList(this.rabbitClient.getExchanges());
	}

	@Override
	public List<Exchange> getExchanges(String vhost) {
		return convertExchangeList(this.rabbitClient.getExchanges(vhost));
	}

	@Override
	public List<Binding> getBindings() {
		return convertBindingList(this.rabbitClient.getBindings());
	}

	@Override
	public List<Binding> getBindings(String vhost) {
		return convertBindingList(this.rabbitClient.getBindings(vhost));
	}

	@Override
	public List<Binding> getBindingsForExchange(String vhost, String exchange) {
		return convertBindingList(this.rabbitClient.getBindingsBySource(vhost, exchange));
	}

	private List<Queue> convertQueueList(List<QueueInfo> queues) {
		return queues.stream()
				.map(this::convert)
				.collect(Collectors.toList());
	}

	private Queue convert(QueueInfo qi) {
		if (qi == null) {
			return null;
		}
		return new Queue(qi.getName(), qi.isDurable(), qi.isExclusive(), qi.isAutoDelete(),
				qi.getArguments());
	}

	private List<Exchange> convertExchangeList(List<ExchangeInfo> exchanges) {
		return exchanges.stream()
				.map(this::convert)
				.collect(Collectors.toList());
	}

	private Exchange convert(ExchangeInfo ei) {
		if (ei == null) {
			return null;
		}
		boolean delayed = false;
		if (ei.getType().equals("x-delayed-message")) {
			ei.setType((String) ei.getArguments().get("x-delayed-type"));
			delayed = true;
		}
		AbstractExchange exchange;
		if (ei.getType().equals("direct")) {
			exchange = new DirectExchange(ei.getName(), ei.isDurable(), ei.isAutoDelete(), ei.getArguments());
		}
		else if (ei.getType().equals("fanout")) {
			exchange = new FanoutExchange(ei.getName(), ei.isDurable(), ei.isAutoDelete(), ei.getArguments());
		}
		else if (ei.getType().equals("headers")) {
			exchange = new HeadersExchange(ei.getName(), ei.isDurable(), ei.isAutoDelete(), ei.getArguments());
		}
		else if (ei.getType().equals("topic")) {
			exchange = new TopicExchange(ei.getName(), ei.isDurable(), ei.isAutoDelete(), ei.getArguments());
		}
		else {
			return null;
		}
		exchange.setDelayed(delayed);
		exchange.setInternal(ei.isInternal());
		return exchange;
	}

	private List<Binding> convertBindingList(List<BindingInfo> bindings) {
		return bindings.stream()
				.map(this::convert)
				.collect(Collectors.toList());
	}

	private Binding convert(BindingInfo bi) {
		if (bi == null) {
			return null;
		}
		return new Binding(bi.getDestination(), DestinationType.valueOf(bi.getDestinationType().toUpperCase()),
				bi.getSource(), bi.getRoutingKey(), bi.getArguments());
	}

}
