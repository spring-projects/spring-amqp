/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.2
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable(queues = { "batch.1", "batch.2" })
public class EnableRabbitBatchIntegrationTests {

	@Autowired
	private BatchingRabbitTemplate template;

	@Autowired
	private Listener listener;

	@Test
	public void simpleList() throws InterruptedException {
		this.template.convertAndSend("batch.1", new Foo("foo"));
		this.template.convertAndSend("batch.1", new Foo("bar"));
		assertThat(this.listener.foosLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.foos.get(0).getBar()).isEqualTo("foo");
		assertThat(this.listener.foos.get(1).getBar()).isEqualTo("bar");
	}

	@Test
	public void messageList() throws InterruptedException {
		this.template.convertAndSend("batch.2", new Foo("foo"));
		this.template.convertAndSend("batch.2", new Foo("bar"));
		assertThat(this.listener.fooMessagesLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.fooMessages.get(0).getPayload().getBar()).isEqualTo("foo");
		assertThat(this.listener.fooMessages.get(1).getPayload().getBar()).isEqualTo("bar");
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(connectionFactory());
			factory.setBatchListener(true);
			return factory;
		}

		@Bean
		public BatchingRabbitTemplate template() {
			return new BatchingRabbitTemplate(connectionFactory(), new SimpleBatchingStrategy(2, 10_000, 10_000L),
					scheduler());
		}

		@Bean
		public ConnectionFactory connectionFactory() {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		}

		@Bean
		public TaskScheduler scheduler() {
			return new ThreadPoolTaskScheduler();
		}

		@Bean
		public Listener listener() {
			return new Listener();
		}

	}

	public static class Listener {

		List<Foo> foos;

		CountDownLatch foosLatch = new CountDownLatch(1);

		List<Message<Foo>> fooMessages;

		CountDownLatch fooMessagesLatch = new CountDownLatch(1);

		@RabbitListener(queues = "batch.1")
		public void listen1(List<Foo> in) {
			this.foos = in;
			this.foosLatch.countDown();
		}

		@RabbitListener(queues = "batch.2")
		public void listen2(List<Message<Foo>> in) {
			this.fooMessages = in;
			this.fooMessagesLatch.countDown();
		}

	}

	@SuppressWarnings("serial")
	public static class Foo implements Serializable {

		private String bar;

		public Foo(String bar) {
			this.bar = bar;
		}

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

	}

}
