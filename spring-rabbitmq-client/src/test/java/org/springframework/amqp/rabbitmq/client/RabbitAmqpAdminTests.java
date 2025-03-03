/*
 * Copyright 2025 the original author or authors.
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

package org.springframework.amqp.rabbitmq.client;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Artem Bilan
 *
 * @since 4.0
 */
@ContextConfiguration
public class RabbitAmqpAdminTests extends RabbitAmqpTestBase {

	@Autowired
	@Qualifier("ds")
	Declarables declarables;

	@Test
	void verifyBeanDeclarations() {
		CompletableFuture<Void> publishFutures =
				CompletableFuture.allOf(
						template.convertAndSend("e1", "k1", "test1"),
						template.convertAndSend("e2", "k2", "test2"),
						template.convertAndSend("e2", "k2", "test3"),
						template.convertAndSend("e3", "k3", "test4"),
						template.convertAndSend("e4", "k4", "test5"));
		assertThat(publishFutures).succeedsWithin(Duration.ofSeconds(10));

		assertThat(template.receiveAndConvert("q1")).succeedsWithin(Duration.ofSeconds(10)).isEqualTo("test1");
		assertThat(template.receiveAndConvert("q2")).succeedsWithin(Duration.ofSeconds(10)).isEqualTo("test2");
		assertThat(template.receiveAndConvert("q2")).succeedsWithin(Duration.ofSeconds(10)).isEqualTo("test3");
		assertThat(template.receiveAndConvert("q3")).succeedsWithin(Duration.ofSeconds(10)).isEqualTo("test4");
		assertThat(template.receiveAndConvert("q4")).succeedsWithin(Duration.ofSeconds(10)).isEqualTo("test5");

		assertThat(declarables.getDeclarablesByType(Queue.class))
				.hasSize(1)
				.extracting(Queue::getName)
				.contains("q4");
		assertThat(declarables.getDeclarablesByType(Exchange.class))
				.hasSize(1)
				.extracting(Exchange::getName)
				.contains("e4");
		assertThat(declarables.getDeclarablesByType(Binding.class))
				.hasSize(1)
				.extracting(Binding::getDestination)
				.contains("q4");
	}

	@Configuration
	public static class Config {

		@Bean
		DirectExchange e1() {
			return new DirectExchange("e1");
		}

		@Bean
		Queue q1() {
			return new Queue("q1");
		}

		@Bean
		Binding b1() {
			return BindingBuilder.bind(q1()).to(e1()).with("k1");
		}

		@Bean
		Declarables es() {
			return new Declarables(
					new DirectExchange("e2"),
					new DirectExchange("e3"));
		}

		@Bean
		Declarables qs() {
			return new Declarables(
					new Queue("q2"),
					new Queue("q3"));
		}

		@Bean
		Declarables bs() {
			return new Declarables(
					new Binding("q2", Binding.DestinationType.QUEUE, "e2", "k2", null),
					new Binding("q3", Binding.DestinationType.QUEUE, "e3", "k3", null));
		}

		@Bean
		Declarables ds() {
			return new Declarables(
					new DirectExchange("e4"),
					new Queue("q4"),
					new Binding("q4", Binding.DestinationType.QUEUE, "e4", "k4", null));
		}

	}

}
