/*
 * Copyright 2002-2025 the original author or authors.
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

package org.springframework.amqp.core;

import java.util.Collections;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Fisher
 * @author Artem Yakshin
 */
public class BindingBuilderTests {

	private static Queue queue;

	@BeforeAll
	public static void setUp() {
		queue = new Queue("q");
	}

	@Test
	public void fanoutBinding() {
		FanoutExchange fanoutExchange = new FanoutExchange("f");
		Binding binding = BindingBuilder.bind(queue).to(fanoutExchange);
		assertThat(binding).isNotNull();
		assertThat(binding.getExchange()).isEqualTo(fanoutExchange.getName());
		assertThat(binding.getRoutingKey()).isEqualTo("");
		assertThat(binding.getDestinationType()).isEqualTo(Binding.DestinationType.QUEUE);
		assertThat(binding.getDestination()).isEqualTo(queue.getName());
	}

	@Test
	public void directBinding() {
		DirectExchange directExchange = new DirectExchange("d");
		String routingKey = "r";
		Binding binding = BindingBuilder.bind(queue).to(directExchange).with(routingKey);
		assertThat(binding).isNotNull();
		assertThat(binding.getExchange()).isEqualTo(directExchange.getName());
		assertThat(binding.getDestinationType()).isEqualTo(Binding.DestinationType.QUEUE);
		assertThat(binding.getDestination()).isEqualTo(queue.getName());
		assertThat(binding.getRoutingKey()).isEqualTo(routingKey);
	}

	@Test
	public void directBindingWithQueueName() {
		DirectExchange directExchange = new DirectExchange("d");
		Binding binding = BindingBuilder.bind(queue).to(directExchange).withQueueName();
		assertThat(binding).isNotNull();
		assertThat(binding.getExchange()).isEqualTo(directExchange.getName());
		assertThat(binding.getDestinationType()).isEqualTo(Binding.DestinationType.QUEUE);
		assertThat(binding.getDestination()).isEqualTo(queue.getName());
		assertThat(binding.getRoutingKey()).isEqualTo(queue.getName());
	}

	@Test
	public void topicBinding() {
		TopicExchange topicExchange = new TopicExchange("t");
		String routingKey = "r";
		Binding binding = BindingBuilder.bind(queue).to(topicExchange).with(routingKey);
		assertThat(binding).isNotNull();
		assertThat(binding.getExchange()).isEqualTo(topicExchange.getName());
		assertThat(binding.getDestinationType()).isEqualTo(Binding.DestinationType.QUEUE);
		assertThat(binding.getDestination()).isEqualTo(queue.getName());
		assertThat(binding.getRoutingKey()).isEqualTo(routingKey);
	}

	@Test
	public void headerBinding() {
		HeadersExchange headersExchange = new HeadersExchange("h");
		String headerKey = "headerKey";
		Binding binding = BindingBuilder.bind(queue).to(headersExchange).where(headerKey).exists();
		assertThat(binding).isNotNull();
		assertThat(binding.getExchange()).isEqualTo(headersExchange.getName());
		assertThat(binding.getDestinationType()).isEqualTo(Binding.DestinationType.QUEUE);
		assertThat(binding.getDestination()).isEqualTo(queue.getName());
		assertThat(binding.getRoutingKey()).isEqualTo("");
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
		assertThat(binding).isNotNull();
		assertThat(binding.getArguments().get("k")).isEqualTo(argumentObject);
		assertThat(binding.getExchange()).isEqualTo(customExchange.getName());
		assertThat(binding.getDestinationType()).isEqualTo(Binding.DestinationType.QUEUE);
		assertThat(binding.getDestination()).isEqualTo(queue.getName());
		assertThat(binding.getRoutingKey()).isEqualTo(routingKey);
	}

	@Test
	public void exchangeBinding() {
		DirectExchange directExchange = new DirectExchange("d");
		FanoutExchange fanoutExchange = new FanoutExchange("f");
		Binding binding = BindingBuilder.bind(directExchange).to(fanoutExchange);
		assertThat(binding).isNotNull();
		assertThat(binding.getExchange()).isEqualTo(fanoutExchange.getName());
		assertThat(binding.getDestinationType()).isEqualTo(Binding.DestinationType.EXCHANGE);
		assertThat(binding.getDestination()).isEqualTo(directExchange.getName());
		assertThat(binding.getRoutingKey()).isEqualTo("");
	}

}
