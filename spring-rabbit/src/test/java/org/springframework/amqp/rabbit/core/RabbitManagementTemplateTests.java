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

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.support.Policy;
import org.springframework.amqp.rabbit.test.BrokerRunning;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;

/**
 * @author Gary Russell
 * @since 1.5
 *
 */
public class RabbitManagementTemplateTests {

	private final CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");

	private final RabbitManagementTemplate template = new RabbitManagementTemplate();

	@ClassRule
	public static BrokerRunning brokerAndManagementRunning = BrokerRunning.isBrokerAndManagementRunning();

	@After
	public void tearDown() {
		connectionFactory.destroy();
	}

	@Test
	public void testAliveness() {
		Map<String, Object> aliveness = this.template.aliveness();
		assertNotNull(aliveness.get("status"));
		assertEquals("ok", aliveness.get("status"));
	}

	@Test
	public void testOverview() {
		Map<String, Object> map = this.template.overview();
		assertTrue(map.containsKey("rabbitmq_version"));
	}

	@Test
	public void testExchanges() {
		List<Exchange> list = this.template.exchanges();
		assertTrue(list.size() > 0);
	}

	@Test
	public void testExchangesVhost() {
		List<Exchange> list = this.template.exchanges("/");
		assertTrue(list.size() > 0);
	}

	@Test
	public void testBindings() {
		List<Binding> list = this.template.bindings();
		assertTrue(list.size() > 0);
	}

	@Test
	public void testBindingsVhost() {
		List<Binding> list = this.template.bindings("/");
		assertTrue(list.size() > 0);
	}

	@Test
	public void testQueues() {
		List<Queue> list = this.template.queues();
		assertTrue(list.size() > 0);
	}

	@Test
	public void testQueuesVhost() {
		List<Queue> list = this.template.queues("/");
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

		List<Binding> bindings = this.template.exchangeSourceBindings("/", exchange1.getName());
		assertEquals(2, bindings.size());
		assertEquals(exchange1.getName(), bindings.get(0).getExchange());
		assertThat("foo", anyOf(equalTo(bindings.get(0).getRoutingKey()), equalTo(bindings.get(1).getRoutingKey())));
		Binding qout = null;
		Binding eout = null;
		if (bindings.get(0).getRoutingKey().equals("foo")) {
			qout = bindings.get(0);
			eout = bindings.get(1);
		}
		else {
			eout = bindings.get(0);
			qout = bindings.get(1);
		}
		assertEquals(Binding.DestinationType.QUEUE, qout.getDestinationType());
		assertEquals(queue.getName(), qout.getDestination());
		assertNotNull(qout.getArguments());
		assertEquals("", qout.getArguments().get("alternate-exchange"));
		assertNotNull(qout.getProperties());
		assertEquals("/", qout.getProperties().get("vhost"));

		assertEquals(Binding.DestinationType.EXCHANGE, eout.getDestinationType());
		assertEquals(exchange2.getName(), eout.getDestination());
		assertNotNull(eout.getProperties());
		assertEquals("/", eout.getProperties().get("vhost"));

		bindings = this.template.exchangeDestBindings("/", exchange2.getName());
		assertEquals(1, bindings.size());
		eout = bindings.get(0);
		assertEquals(Binding.DestinationType.EXCHANGE, eout.getDestinationType());
		assertEquals(exchange1.getName(), eout.getExchange());
		assertEquals(exchange2.getName(), eout.getDestination());

		bindings = this.template.exchangeToQueueBindings("/", exchange1.getName(), queue.getName());
		assertEquals(1, bindings.size());
		qout = bindings.get(0);
		assertEquals(Binding.DestinationType.QUEUE, qout.getDestinationType());
		assertEquals(exchange1.getName(), qout.getExchange());
		assertEquals(queue.getName(), qout.getDestination());

		bindings = this.template.exchangeToExchangeBindings("/", exchange1.getName(), exchange2.getName());
		assertEquals(1, bindings.size());
		eout = bindings.get(0);
		assertEquals(Binding.DestinationType.EXCHANGE, eout.getDestinationType());
		assertEquals(exchange1.getName(), eout.getExchange());
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
		Exchange exchangeOut = this.template.exchange("/", exchange.getName());
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
		Queue queue = new Queue(UUID.randomUUID().toString(), true, false, true, args);
		admin.declareQueue(queue);
		Channel channel = this.connectionFactory.createConnection().createChannel(false);
		String consumer = channel.basicConsume(queue.getName(), false, "", false, true, null, new DefaultConsumer(channel));
		Queue queueOut = this.template.queue("/", queue.getName());
		int n = 0;
		while (n++ < 100 && queueOut.getProperties().get("exclusive_consumer_tag").equals("")) {
			Thread.sleep(100);
			queueOut = template.queue("/", queue.getName());
		}
		assertTrue(queueOut.isDurable());
		assertTrue(queueOut.isAutoDelete());
		assertEquals(queue.getName(), queueOut.getName());
		assertEquals(args, queueOut.getArguments());
		assertEquals(consumer, queueOut.getProperties().get("exclusive_consumer_tag"));
		channel.basicCancel(consumer);
		channel.close();
		admin.deleteQueue(queue.getName());
	}

	@Test
	public void testGenericGet() {
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Exchange exchange = new DirectExchange(UUID.randomUUID().toString(), false, true);
		admin.declareExchange(exchange);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> connections = this.template.executeGet(List.class, new String[] { "connections" });
		assertTrue(connections.size() > 0);
		admin.deleteExchange(exchange.getName());
	}

	@Test
	public void testPolicies() {
		String name = UUID.randomUUID().toString();
		Policy policy = new Policy("/", name, "^foo.", null, 0,
				Collections.<String, Object>singletonMap("message-ttl", 1000));
		this.template.addPolicy(policy);
		List<Policy> list = this.template.policies();
		assertTrue(list.size() > 0);
		Policy out = null;
		for (Policy policyOut : list) {
			if (policyOut.getName().equals(name)) {
				out = policyOut;
				break;
			}
		}
		assertNotNull(out);

		list = this.template.policies("/");
		assertTrue(list.size() > 0);
		out = null;
		for (Policy policyOut : list) {
			if (policyOut.getName().equals(name)) {
				out = policyOut;
				break;
			}
		}
		assertNotNull(out);

		this.template.removePolicy("/", name);
		list = this.template.policies();
		out = null;
		for (Policy policyOut : list) {
			if (policyOut.getName().equals(name)) {
				out = policyOut;
				break;
			}
		}
		assertNull(out);
	}

}
