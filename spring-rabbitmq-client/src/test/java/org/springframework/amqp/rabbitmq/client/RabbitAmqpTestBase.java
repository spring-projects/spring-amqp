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

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;

import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.beans.factory.annotation.Autowired;
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
abstract class RabbitAmqpTestBase extends AbstractTestContainerTests {

	@Autowired
	protected Environment environment;

	@Autowired
	protected Connection connection;

	@Autowired
	protected RabbitAmqpAdmin admin;

	@Autowired
	protected RabbitAmqpTemplate template;

	@Configuration
	public static class AmqpCommonConfig {

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

	}

}
