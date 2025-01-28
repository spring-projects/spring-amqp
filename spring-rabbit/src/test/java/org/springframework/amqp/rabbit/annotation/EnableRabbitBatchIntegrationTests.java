/*
 * Copyright 2019-2025 the original author or authors.
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.2
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable(queues = { "batch.1", "batch.2", "batch.3", "batch.4", "batch.5" })
public class EnableRabbitBatchIntegrationTests {

	@Autowired
	private BatchingRabbitTemplate template;

	@Autowired
	private Listener listener;

	@BeforeAll
	static void setup() {
		System.setProperty("spring.amqp.deserialization.trust.all", "true");
	}

	@Test
	public void simpleList() throws InterruptedException {
		this.template.convertAndSend("batch.1", new Foo("foo"));
		this.template.convertAndSend("batch.1", new Foo("bar"));
		assertThat(this.listener.foosLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.foos).hasSize(2);
		assertThat(this.listener.foos.get(0).getBar()).isEqualTo("foo");
		assertThat(this.listener.foos.get(1).getBar()).isEqualTo("bar");
	}

	@Test
	public void messageList() throws InterruptedException {
		this.template.convertAndSend("batch.2", new Foo("foo"));
		this.template.convertAndSend("batch.2", new Foo("bar"));
		assertThat(this.listener.fooMessagesLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.fooMessages).hasSize(2);
		assertThat(this.listener.fooMessages.get(0).getPayload().getBar()).isEqualTo("foo");
		assertThat(this.listener.fooMessages.get(0).getHeaders().get(AmqpHeaders.LAST_IN_BATCH, Boolean.class))
				.isFalse();
		assertThat(this.listener.fooMessages.get(1).getPayload().getBar()).isEqualTo("bar");
		assertThat(this.listener.fooMessages.get(1).getHeaders().get(AmqpHeaders.LAST_IN_BATCH, Boolean.class))
				.isTrue();
		assertThat(this.listener.fooMessages.get(1).getHeaders().get(AmqpHeaders.BATCH_SIZE, Integer.class))
				.isEqualTo(2);
	}

	@Test
	public void simpleListConsumerAndProducerBatching() throws InterruptedException {
		this.template.convertAndSend("batch.3", new Foo("foo"));
		this.template.convertAndSend("batch.3", new Foo("bar"));
		this.template.convertAndSend("batch.3", new Foo("baz"));
		this.template.convertAndSend("batch.3", new Foo("qux"));
		assertThat(this.listener.fooConsumerBatchTooLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.foosConsumerBatchToo).hasSize(4);
		assertThat(this.listener.foosConsumerBatchToo.get(0).getBar()).isEqualTo("foo");
		assertThat(this.listener.foosConsumerBatchToo.get(1).getBar()).isEqualTo("bar");
		assertThat(this.listener.foosConsumerBatchToo.get(2).getBar()).isEqualTo("baz");
		assertThat(this.listener.foosConsumerBatchToo.get(3).getBar()).isEqualTo("qux");
	}

	@Test
	public void nativeMessageList() throws InterruptedException {
		this.template.convertAndSend("batch.4", new Foo("foo"));
		this.template.convertAndSend("batch.4", new Foo("bar"));
		assertThat(this.listener.nativeMessagesLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.nativeMessages).hasSize(2);
		Foo payload = (Foo) new SimpleMessageConverter().fromMessage(this.listener.nativeMessages.get(0));
		assertThat(payload.getBar()).isEqualTo("foo");
		assertThat(this.listener.nativeMessages.get(1).getMessageProperties()
				.getHeaders()
				.get(AmqpHeaders.BATCH_SIZE))
				.isEqualTo(2);
	}

	@Test
	public void collectionWithStringInfer() throws InterruptedException {
		this.template.convertAndSend("batch.5", new Foo("foo"));
		this.template.convertAndSend("batch.5", new Foo("bar"));
		assertThat(this.listener.fivesLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.fives).hasSize(2);
		assertThat(this.listener.fives.get(0).getBar()).isEqualTo("foo");
		assertThat(this.listener.fives.get(1).getBar()).isEqualTo("bar");
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
		public DirectRabbitListenerContainerFactory directListenerContainerFactory() {
			DirectRabbitListenerContainerFactory factory = new DirectRabbitListenerContainerFactory();
			factory.setConnectionFactory(connectionFactory());
			factory.setBatchListener(true);
			factory.setContainerCustomizer(container -> {
				if (container.getQueueNames()[0].equals("batch.4")) {
					container.setDeBatchingEnabled(true);
				}
			});
			return factory;
		}

		@Bean
		public SimpleRabbitListenerContainerFactory consumerBatchContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(connectionFactory());
			factory.setBatchListener(false);
			factory.setConsumerBatchEnabled(true);
			factory.setBatchSize(2);
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

		List<Foo> foosConsumerBatchToo;

		CountDownLatch fooConsumerBatchTooLatch = new CountDownLatch(1);

		List<Foo> fives = new ArrayList<>();

		CountDownLatch fivesLatch = new CountDownLatch(1);

		private List<org.springframework.amqp.core.Message> nativeMessages;

		private final CountDownLatch nativeMessagesLatch = new CountDownLatch(1);

		@RabbitListener(queues = "batch.1")
		public void listen1(List<Foo> in) {
			this.foos = in;
			this.foosLatch.countDown();
		}

		@RabbitListener(queues = "batch.2", containerFactory = "directListenerContainerFactory")
		public void listen2(List<Message<Foo>> in) {
			this.fooMessages = in;
			this.fooMessagesLatch.countDown();
		}

		@RabbitListener(queues = "batch.3", containerFactory = "consumerBatchContainerFactory", batch = "true")
		public void listen3(List<Foo> in) {
			this.foosConsumerBatchToo = in;
			this.fooConsumerBatchTooLatch.countDown();
		}

		@RabbitListener(queues = "batch.4", containerFactory = "directListenerContainerFactory")
		public void listen4(List<org.springframework.amqp.core.Message> in) {
			this.nativeMessages = in;
			this.nativeMessagesLatch.countDown();
		}

		@RabbitListener(queues = "batch.5")
		public void listen5(Collection<Foo> in) {
			this.fives.addAll(in);
			this.fivesLatch.countDown();
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
