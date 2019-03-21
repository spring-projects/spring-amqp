/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.junit.BrokerRunning;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.domain.BindingInfo;
import com.rabbitmq.http.client.domain.ExchangeInfo;
import com.rabbitmq.http.client.domain.QueueInfo;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.5
 *
 */
public class RabbitRestApiTests {

	private final CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");

	private final Client rabbitRestClient;


	@ClassRule
	public static BrokerRunning brokerAndManagementRunning = BrokerRunning.isBrokerAndManagementRunning();

	public RabbitRestApiTests() throws MalformedURLException, URISyntaxException {
		this.rabbitRestClient = new Client("http://localhost:15672/api/", "guest", "guest");
	}

	@After
	public void tearDown() {
		connectionFactory.destroy();
	}

	@Test
	public void testExchanges() {
		List<ExchangeInfo> list = this.rabbitRestClient.getExchanges();
		assertTrue(list.size() > 0);
	}

	@Test
	public void testExchangesVhost() {
		List<ExchangeInfo> list = this.rabbitRestClient.getExchanges("/");
		assertTrue(list.size() > 0);
	}

	@Test
	public void testBindings() {
		List<BindingInfo> list = this.rabbitRestClient.getBindings();
		assertTrue(list.size() > 0);
	}

	@Test
	public void testBindingsVhost() {
		List<BindingInfo> list = this.rabbitRestClient.getBindings("/");
		assertTrue(list.size() > 0);
	}

	@Test
	public void testQueues() {
		List<QueueInfo> list = this.rabbitRestClient.getQueues();
		assertTrue(list.size() > 0);
	}

	@Test
	public void testQueuesVhost() {
		List<QueueInfo> list = this.rabbitRestClient.getQueues("/");
		assertTrue(list.size() > 0);
	}

	@Test
	public void testBindingsDetail() {
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Map<String, Object> args = Collections.<String, Object>singletonMap("alternate-exchange", "");
		Exchange exchange1 = new DirectExchange(UUID.randomUUID().toString(), false, true, args);
		admin.declareExchange(exchange1);
		Exchange exchange2 = new DirectExchange(UUID.randomUUID().toString(), false, true, args);
		admin.declareExchange(exchange2);
		Queue queue = admin.declareQueue();
		Binding binding1 = BindingBuilder
				.bind(queue)
				.to(exchange1)
				.with("foo")
				.and(args);
		admin.declareBinding(binding1);
		Binding binding2 = BindingBuilder
				.bind(exchange2)
				.to((DirectExchange) exchange1)
				.with("bar");
		admin.declareBinding(binding2);

		List<BindingInfo> bindings = this.rabbitRestClient.getBindingsBySource("/", exchange1.getName());
		assertEquals(2, bindings.size());
		assertEquals(exchange1.getName(), bindings.get(0).getSource());
		assertThat("foo", anyOf(equalTo(bindings.get(0).getRoutingKey()), equalTo(bindings.get(1).getRoutingKey())));
		BindingInfo qout = null;
		BindingInfo eout = null;
		if (bindings.get(0).getRoutingKey().equals("foo")) {
			qout = bindings.get(0);
			eout = bindings.get(1);
		}
		else {
			eout = bindings.get(0);
			qout = bindings.get(1);
		}
		assertEquals("queue", qout.getDestinationType());
		assertEquals(queue.getName(), qout.getDestination());
		assertNotNull(qout.getArguments());
		assertEquals("", qout.getArguments().get("alternate-exchange"));

		assertEquals("exchange", eout.getDestinationType());
		assertEquals(exchange2.getName(), eout.getDestination());

		admin.deleteExchange(exchange1.getName());
		admin.deleteExchange(exchange2.getName());
	}

	@Test
	public void testSpecificExchange() {
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Map<String, Object> args = Collections.<String, Object>singletonMap("alternate-exchange", "");
		Exchange exchange = new DirectExchange(UUID.randomUUID().toString(), true, true, args);
		admin.declareExchange(exchange);
		ExchangeInfo exchangeOut = this.rabbitRestClient.getExchange("/", exchange.getName());
		assertTrue(exchangeOut.isDurable());
		assertTrue(exchangeOut.isAutoDelete());
		assertEquals(exchange.getName(), exchangeOut.getName());
		assertEquals(args, exchangeOut.getArguments());
		admin.deleteExchange(exchange.getName());
	}

	@Test
	public void testSpecificQueue() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Map<String, Object> args = Collections.<String, Object>singletonMap("foo", "bar");
		Queue queue1 = QueueBuilder.nonDurable(UUID.randomUUID().toString())
				.autoDelete()
				.withArguments(args)
				.build();
		admin.declareQueue(queue1);
		Queue queue2 = QueueBuilder.durable(UUID.randomUUID().toString())
				.withArguments(args)
				.build();
		admin.declareQueue(queue2);
		Channel channel = this.connectionFactory.createConnection().createChannel(false);
		String consumer = channel.basicConsume(queue1.getName(), false, "", false, true, null, new DefaultConsumer(channel));
		QueueInfo qi = this.rabbitRestClient.getQueue("/", queue1.getName());
		int n = 0;
		while (n++ < 100 && (qi.getExclusiveConsumerTag() == null || qi.getExclusiveConsumerTag().equals(""))) {
			Thread.sleep(100);
			qi = this.rabbitRestClient.getQueue("/", queue1.getName());
		}
		QueueInfo queueOut = this.rabbitRestClient.getQueue("/", queue1.getName());
		assertFalse(queueOut.isDurable());
		assertFalse(queueOut.isExclusive());
		assertTrue(queueOut.isAutoDelete());
		assertEquals(queue1.getName(), queueOut.getName());
		assertEquals(args, queueOut.getArguments());
		assertEquals(consumer, qi.getExclusiveConsumerTag());
		channel.basicCancel(consumer);
		channel.close();

		queueOut = this.rabbitRestClient.getQueue("/", queue2.getName());
		assertTrue(queueOut.isDurable());
		assertFalse(queueOut.isExclusive());
		assertFalse(queueOut.isAutoDelete());
		assertEquals(queue2.getName(), queueOut.getName());
		assertEquals(args, queueOut.getArguments());

		admin.deleteQueue(queue1.getName());
		admin.deleteQueue(queue2.getName());
	}

	@Test
	public void testDeleteExchange() {
		String exchangeName = "testExchange";
		Exchange testExchange = new DirectExchange(exchangeName);
		ExchangeInfo info = new ExchangeInfo();
		info.setArguments(testExchange.getArguments());
		info.setAutoDelete(testExchange.isAutoDelete());
		info.setDurable(testExchange.isDurable());
		info.setType(testExchange.getType());
		this.rabbitRestClient.declareExchange("/", testExchange.getName(), info);
		ExchangeInfo exchangeToAssert = this.rabbitRestClient.getExchange("/", exchangeName);
		assertEquals(testExchange.getName(), exchangeToAssert.getName());
		assertEquals(testExchange.getType(), exchangeToAssert.getType());
		this.rabbitRestClient.deleteExchange("/", testExchange.getName());
		assertNull(this.rabbitRestClient.getExchange("/", exchangeName));
	}

}
