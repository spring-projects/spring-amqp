/*
 * Copyright 2002-2017 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Mark Fisher
 * @author Artem Yakshin
 */
public class BindingBuilderTests {

	private static Queue queue;

	@BeforeClass
	public static void setUp() {
		queue = new Queue("q");
	}

	@Test
	public void fanoutBinding() {
		FanoutExchange fanoutExchange = new FanoutExchange("f");
		Binding binding = BindingBuilder.bind(queue).to(fanoutExchange);
		assertNotNull(binding);
		assertEquals(fanoutExchange.getName(), binding.getExchange());
		assertEquals("", binding.getRoutingKey());
		assertEquals(Binding.DestinationType.QUEUE, binding.getDestinationType());
		assertEquals(queue.getName(), binding.getDestination());
	}

	@Test
	public void directBinding() {
		DirectExchange directExchange = new DirectExchange("d");
		String routingKey = "r";
		Binding binding = BindingBuilder.bind(queue).to(directExchange).with(routingKey);
		assertNotNull(binding);
		assertEquals(directExchange.getName(), binding.getExchange());
		assertEquals(Binding.DestinationType.QUEUE, binding.getDestinationType());
		assertEquals(queue.getName(), binding.getDestination());
		assertEquals(routingKey, binding.getRoutingKey());
	}

	@Test
	public void directBindingWithQueueName() {
		DirectExchange directExchange = new DirectExchange("d");
		Binding binding = BindingBuilder.bind(queue).to(directExchange).withQueueName();
		assertNotNull(binding);
		assertEquals(directExchange.getName(), binding.getExchange());
		assertEquals(Binding.DestinationType.QUEUE, binding.getDestinationType());
		assertEquals(queue.getName(), binding.getDestination());
		assertEquals(queue.getName(), binding.getRoutingKey());
	}

	@Test
	public void topicBinding() {
		TopicExchange topicExchange = new TopicExchange("t");
		String routingKey = "r";
		Binding binding = BindingBuilder.bind(queue).to(topicExchange).with(routingKey);
		assertNotNull(binding);
		assertEquals(topicExchange.getName(), binding.getExchange());
		assertEquals(Binding.DestinationType.QUEUE, binding.getDestinationType());
		assertEquals(queue.getName(), binding.getDestination());
		assertEquals(routingKey, binding.getRoutingKey());
	}

	@Test
	public void headerBinding() {
		HeadersExchange headersExchange = new HeadersExchange("h");
		String headerKey = "headerKey";
		Binding binding = BindingBuilder.bind(queue).to(headersExchange).where(headerKey).exists();
		assertNotNull(binding);
		assertEquals(headersExchange.getName(), binding.getExchange());
		assertEquals(Binding.DestinationType.QUEUE, binding.getDestinationType());
		assertEquals(queue.getName(), binding.getDestination());
		assertEquals("", binding.getRoutingKey());
	}

	@Test
	public void customBinding() {
		class CustomExchange extends AbstractExchange {
			CustomExchange(String name) {
				super(name);
			}

			@Override
			public String getType() {
				return "x-custom";
			}
		}
		Object argumentObject = new Object();
		CustomExchange customExchange = new CustomExchange("c");
		String routingKey = "r";
		Binding binding = BindingBuilder.//
				bind(queue).//
				to(customExchange).//
				with(routingKey).//
				and(Collections.<String, Object>singletonMap("k", argumentObject));
		assertNotNull(binding);
		assertEquals(argumentObject, binding.getArguments().get("k"));
		assertEquals(customExchange.getName(), binding.getExchange());
		assertEquals(Binding.DestinationType.QUEUE, binding.getDestinationType());
		assertEquals(queue.getName(), binding.getDestination());
		assertEquals(routingKey, binding.getRoutingKey());
	}

	@Test
	public void exchangeBinding() {
		DirectExchange directExchange = new DirectExchange("d");
		FanoutExchange fanoutExchange = new FanoutExchange("f");
		Binding binding = BindingBuilder.bind(directExchange).to(fanoutExchange);
		assertNotNull(binding);
		assertEquals(fanoutExchange.getName(), binding.getExchange());
		assertEquals(Binding.DestinationType.EXCHANGE, binding.getDestinationType());
		assertEquals(directExchange.getName(), binding.getDestination());
		assertEquals("", binding.getRoutingKey());
	}

}
