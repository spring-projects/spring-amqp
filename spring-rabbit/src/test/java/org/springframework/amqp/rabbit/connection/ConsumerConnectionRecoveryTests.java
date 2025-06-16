/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * @author Artem Bilan
 *
 * @since 3.0.11
 *
 */
@SpringJUnitConfig
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext
public class ConsumerConnectionRecoveryTests {

	@Container
	static final RabbitMQContainer RABBIT_MQ_CONTAINER =
			new RabbitMQContainer(DockerImageName.parse("rabbitmq"));

	@Test
	void verifyThatChannelPermitsAreReleaseOnReconnect(@Autowired TestConfiguration application)
			throws InterruptedException {

		application.rabbitTemplate().convertAndSend("testQueue", "test data #1");

		assertThat(application.received.poll(20, TimeUnit.SECONDS)).isEqualTo("test data #1");

		RABBIT_MQ_CONTAINER.stop();
		RABBIT_MQ_CONTAINER.start();

		application.connectionFactory().setPort(RABBIT_MQ_CONTAINER.getAmqpPort());
		application.publisherConnectionFactory().setPort(RABBIT_MQ_CONTAINER.getAmqpPort());

		application.rabbitTemplate().convertAndSend("testQueue", "test data #2");

		assertThat(application.received.poll(30, TimeUnit.SECONDS)).isEqualTo("test data #2");
	}

	@Configuration
	@EnableRabbit
	public static class TestConfiguration {

		@Bean
		CachingConnectionFactory connectionFactory() {
			CachingConnectionFactory connectionFactory =
					new CachingConnectionFactory("localhost", RABBIT_MQ_CONTAINER.getAmqpPort());
			connectionFactory.setChannelCacheSize(1);
			connectionFactory.setChannelCheckoutTimeout(2000);
			return connectionFactory;
		}

		@Bean
		CachingConnectionFactory publisherConnectionFactory() {
			CachingConnectionFactory connectionFactory =
					new CachingConnectionFactory("localhost", RABBIT_MQ_CONTAINER.getAmqpPort());
			connectionFactory.setChannelCacheSize(1);
			connectionFactory.setChannelCheckoutTimeout(2000);
			return connectionFactory;
		}

		@Bean
		RabbitTemplate rabbitTemplate() {
			return new RabbitTemplate(publisherConnectionFactory());
		}

		@Bean
		SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(connectionFactory());
			return factory;
		}

		@Bean
		RabbitAdmin rabbitAdmin() {
			return new RabbitAdmin(publisherConnectionFactory());
		}

		BlockingQueue<String> received = new LinkedBlockingQueue<>();

		@RabbitListener(queuesToDeclare = @Queue("testQueue"))
		void consume(String payload) {
			this.received.add(payload);
		}

	}

}

