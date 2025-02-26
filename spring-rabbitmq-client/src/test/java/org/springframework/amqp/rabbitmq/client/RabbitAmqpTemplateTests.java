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

import com.rabbitmq.client.amqp.Connection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * @author Artem Bilan
 *
 * @since 4.0
 */
@ContextConfiguration
public class RabbitAmqpTemplateTests extends RabbitAmqpTestBase {

	@Autowired
	Connection connection;

	RabbitAmqpTemplate rabbitAmqpTemplate;

	@BeforeEach
	void setUp() {
		this.rabbitAmqpTemplate = new RabbitAmqpTemplate(this.connection);
		this.rabbitAmqpTemplate.afterPropertiesSet();
	}

	@AfterEach
	void tearDown() {
		this.rabbitAmqpTemplate.destroy();
	}

	@Test
	void illegalStateOnNoDefaults() {
		assertThatIllegalStateException()
				.isThrownBy(() -> this.template.send(new Message(new byte[0])))
				.withMessage("For send with defaults, an 'exchange' (and optional 'key') or 'queue' must be provided");

		assertThatIllegalStateException()
				.isThrownBy(() -> this.template.convertAndSend(new byte[0]))
				.withMessage("For send with defaults, an 'exchange' (and optional 'key') or 'queue' must be provided");
	}

	@Test
	void defaultExchangeAndRoutingKey() {
		this.rabbitAmqpTemplate.setExchange("e1");
		this.rabbitAmqpTemplate.setKey("k1");

		assertThat(this.rabbitAmqpTemplate.convertAndSend("test1"))
				.succeedsWithin(Duration.ofSeconds(10));

		assertThat(this.rabbitAmqpTemplate.receiveAndConvert("q1"))
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo("test1");
	}

	@Test
	void defaultQueues() {
		this.rabbitAmqpTemplate.setQueue("q1");
		this.rabbitAmqpTemplate.setDefaultReceiveQueue("q1");

		assertThat(this.rabbitAmqpTemplate.convertAndSend("test2"))
				.succeedsWithin(Duration.ofSeconds(10));

		assertThat(this.rabbitAmqpTemplate.receiveAndConvert())
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo("test2");
	}

	@Configuration
	static class Config {

		@Bean
		DirectExchange e1() {
			return new DirectExchange("e1", false, false);
		}

		@Bean
		Queue q1() {
			return new Queue("q1", false, false, false);
		}

		@Bean
		Binding b1() {
			return BindingBuilder.bind(q1()).to(e1()).with("k1");
		}

	}

}
