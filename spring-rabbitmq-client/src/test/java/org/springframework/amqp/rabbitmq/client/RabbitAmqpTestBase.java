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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;

import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.Lifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * The {@link AbstractTestContainerTests} extension
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
@SpringJUnitConfig
@DirtiesContext
public abstract class RabbitAmqpTestBase extends AbstractTestContainerTests {

	@Autowired
	protected Environment environment;

	@Autowired
	protected Connection connection;

	@Autowired
	protected RabbitAmqpAdmin admin;

	@Autowired
	protected RabbitAmqpTemplate template;

	@Configuration
	public static class AmqpCommonConfig implements Lifecycle {

		@Autowired
		List<Declarable> declarables;

		@Autowired(required = false)
		List<Declarables> declarableContainers = new ArrayList<>();

		@Autowired
		RabbitAmqpAdmin admin;

		@Bean
		Environment environment() {
			return new AmqpEnvironmentBuilder()
					.connectionSettings()
					.port(amqpPort())
					.environmentBuilder()
					.build();
		}

		@Bean
		AmqpConnectionFactoryBean connection(Environment environment) {
			return new AmqpConnectionFactoryBean(environment);
		}

		@Bean
		RabbitAmqpAdmin admin(Connection connection) {
			return new RabbitAmqpAdmin(connection);
		}

		@Bean
		RabbitAmqpTemplate rabbitTemplate(Connection connection) {
			return new RabbitAmqpTemplate(connection);
		}

		volatile boolean running;

		@Override
		public void start() {
			this.running = true;
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}

		@Override
		public void stop() {
			Stream.concat(this.declarables.stream(),
							this.declarableContainers.stream()
									.flatMap((declarables) -> declarables.getDeclarables().stream()))
					.filter((declarable) -> declarable instanceof Queue || declarable instanceof Exchange)
					.forEach((declarable) -> {
						if (declarable instanceof Queue queue) {
							this.admin.deleteQueue(queue.getName());
						}
						else {
							this.admin.deleteExchange(((Exchange) declarable).getName());
						}
					});
		}

	}

}
