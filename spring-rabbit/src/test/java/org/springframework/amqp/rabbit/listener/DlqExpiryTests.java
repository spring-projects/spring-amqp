/*
 * Copyright 2018-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.1
 *
 */
@RabbitAvailable
@SpringJUnitConfig
@DirtiesContext
public class DlqExpiryTests {

	@Autowired
	private Config config;

	@Test
	public void testExpiredDies() throws Exception {
		this.config.template().convertAndSend("test.expiry.main", "foo");
		assertThat(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(300);
		assertThat(this.config.counter).isEqualTo(2);
		this.config.admin().deleteQueue("test.expiry.dlq");
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		private int counter;

		private final CountDownLatch latch = new CountDownLatch(2);

		@Bean
		public CachingConnectionFactory ccf() {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		}

		@Bean
		public RabbitAdmin admin() {
			return new RabbitAdmin(ccf());
		}

		@Bean
		public RabbitTemplate template() {
			return new RabbitTemplate(ccf());
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(ccf());
			return factory;
		}

		@Bean
		public Queue main() {
			return QueueBuilder.nonDurable("test.expiry.main").autoDelete()
					.withArgument("x-dead-letter-exchange", "")
					.withArgument("x-dead-letter-routing-key", "test.expiry.dlq")
					.build();
		}

		@Bean
		public Queue dlq() {
			return QueueBuilder.nonDurable("test.expiry.dlq").autoDelete()
					.withArgument("x-dead-letter-exchange", "")
					.withArgument("x-dead-letter-routing-key", "test.expiry.main")
					.withArgument("x-message-ttl", 100)
					.build();
		}

		@RabbitListener(queues = "test.expiry.main")
		public void listen(Message in) {
			this.latch.countDown();
			this.counter++;
			throw new MessageConversionException("test.expiry");
		}

	}

}
