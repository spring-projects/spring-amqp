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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.test.BrokerRunning;

/**
 * @author Gary Russell
 * @since 1.5
 *
 */
public class RabbitAdminTemplateTests {

	@ClassRule
	public static BrokerRunning brokerRunning = BrokerRunning.isRunning();

	@Test
	public void testOverview() {
		RabbitAdminTemplate template = new RabbitAdminTemplate();
		Map<String, Object> map = template.overview();
		assertTrue(map.containsKey("rabbitmq_version"));
	}

	@Test
	public void testExchanges() {
		RabbitAdminTemplate template = new RabbitAdminTemplate();
		List<Map<String, Object>> list = template.exchanges();
		assertTrue(list.size() > 0);
	}

	@Test
	public void testExchangesVhost() {
		RabbitAdminTemplate template = new RabbitAdminTemplate();
		List<Map<String, Object>> list = template.exchanges("/");
		assertTrue(list.size() > 0);
	}

	@Test
	public void testBindings() {
		RabbitAdmin admin = new RabbitAdmin(new CachingConnectionFactory("localhost"));
		Exchange exchange = new DirectExchange(UUID.randomUUID().toString(), false, true);
		admin.declareExchange(exchange);
		Queue queue = admin.declareQueue();
		Binding binding = BindingBuilder
				.bind(queue)
				.to(exchange)
				.with("foo")
				.and(Collections.<String, Object>singletonMap("alternate-exchange", ""));
		admin.declareBinding(binding);
		RabbitAdminTemplate template = new RabbitAdminTemplate();
		List<Binding> bindings = template.exchangeBindings("/", exchange.getName());
		assertEquals(1, bindings.size());
		assertEquals(exchange.getName(), bindings.get(0).getExchange());
		assertEquals("foo", bindings.get(0).getRoutingKey());
		assertEquals(Binding.DestinationType.QUEUE, bindings.get(0).getDestinationType());
		assertEquals(queue.getName(), bindings.get(0).getDestination());
		assertNotNull(bindings.get(0).getArguments());
		assertEquals("", bindings.get(0).getArguments().get("alternate-exchange"));
		assertNotNull(bindings.get(0).getProperties());
		assertEquals("/", bindings.get(0).getProperties().get("vhost"));
	}

}
