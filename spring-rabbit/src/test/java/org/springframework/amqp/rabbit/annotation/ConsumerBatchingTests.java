/*
 * Copyright 2019-present the original author or authors.
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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Gary Russell
 * @since 5.2
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable(queues = { "c.batch.1", "c.batch.2" })
public class ConsumerBatchingTests {

	@Autowired
	private RabbitTemplate template;

	@Autowired
	private Listener listener;

	@Autowired
	private MeterRegistry meterRegistry;

	@Test
	public void replayWholeBatch() throws InterruptedException {
		this.template.convertAndSend("c.batch.1", new Foo("foo"));
		this.template.convertAndSend("c.batch.1", new Foo("bar"));
		this.template.convertAndSend("c.batch.1", new Foo("baz"));
		this.template.convertAndSend("c.batch.1", new Foo("qux"));
		assertThat(this.listener.foosLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.foos).hasSize(8);
		assertThat(this.listener.foos)
			.extracting(foo -> foo.getBar())
			.contains("foo", "bar", "baz", "qux", "foo", "bar", "baz", "qux");
		Timer timer = await().until(() -> {
			try {
				return this.meterRegistry.get("spring.rabbitmq.listener")
						.tag("listener.id", "batch.1")
						.tag("queue", "[c.batch.1]")
						.tag("result", "success")
						.tag("exception", "none")
						.tag("extraTag", "foo")
						.timer();
			}
			catch (@SuppressWarnings("unused") Exception e) {
				return null;
			}
		}, tim -> tim != null);
		assertThat(timer).isNotNull();
		assertThat(timer.count()).isEqualTo(1L);
		timer = this.meterRegistry.get("spring.rabbitmq.listener")
				.tag("listener.id", "batch.1")
				.tag("queue", "[c.batch.1]")
				.tag("result", "failure")
				.tag("exception", "ListenerExecutionFailedException")
				.tag("extraTag", "foo")
				.timer();
		assertThat(timer).isNotNull();
		assertThat(timer.count()).isEqualTo(1L);
	}

	@Test
	public void replayHalfBatch() throws InterruptedException {
		this.template.convertAndSend("c.batch.2", new Foo("foo"));
		this.template.convertAndSend("c.batch.2", new Foo("bar"));
		this.template.convertAndSend("c.batch.2", new Foo("baz"));
		this.template.convertAndSend("c.batch.2", new Foo("qux"));
		assertThat(this.listener.fooMessagesLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.fooMessages).hasSize(6);
		assertThat(this.listener.fooMessages)
			.extracting(msg -> msg.getPayload().getBar())
			.contains("foo", "bar", "baz", "qux", "baz", "qux");
	}

	@Test
	public void rejectWholeBatch() throws InterruptedException {
		this.template.convertAndSend("c.batch.3", new Foo("foo"));
		this.template.convertAndSend("c.batch.3", new Foo("bar"));
		this.template.convertAndSend("c.batch.3", new Foo("baz"));
		this.template.convertAndSend("c.batch.3", new Foo("qux"));
		assertThat(this.listener.dlqLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.dlqd).hasSize(4);
		assertThat(this.listener.dlqd)
			.extracting(foo -> foo.getBar())
			.contains("foo", "bar", "baz", "qux");
	}

	@Test
	public void rejectHalfBatch() throws InterruptedException {
		this.template.convertAndSend("c.batch.4", new Foo("foo"));
		this.template.convertAndSend("c.batch.4", new Foo("bar"));
		this.template.convertAndSend("c.batch.4", new Foo("baz"));
		this.template.convertAndSend("c.batch.4", new Foo("qux"));
		assertThat(this.listener.dlqHalfLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.dlqHalf).hasSize(2);
		assertThat(this.listener.dlqHalf)
			.extracting(foo -> foo.getBar())
			.contains("baz", "qux");
	}

	@Test
	public void rejectOneReplayRest() throws InterruptedException {
		this.template.convertAndSend("c.batch.5", new Foo("foo"));
		this.template.convertAndSend("c.batch.5", new Foo("bar"));
		this.template.convertAndSend("c.batch.5", new Foo("baz"));
		this.template.convertAndSend("c.batch.5", new Foo("qux"));
		assertThat(this.listener.dlqOneRejectedLatch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.dlqOneRejectedMessages).hasSize(7);
		assertThat(this.listener.dlqOneRejectedMessages)
			.extracting(foo -> foo.getPayload().getBar())
			.contains("foo", "bar", "baz", "qux", "foo", "baz", "qux");
		assertThat(this.listener.dlqOneRejectedLatch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.dlqOneRejected)
			.extracting(foo -> foo.getBar())
			.contains("bar");
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		@Bean
		public MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(connectionFactory());
			factory.setBatchListener(true);
			factory.setConsumerBatchEnabled(true);
			factory.setBatchSize(4);
			factory.setContainerCustomizer(
					container -> container.setMicrometerTags(Collections.singletonMap("extraTag", "foo")));
			return factory;
		}

		@Bean
		public RabbitAdmin admin() {
			return new RabbitAdmin(template());
		}

		@Bean
		public RabbitTemplate template() {
			return new RabbitTemplate(connectionFactory());
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

		@Bean
		public org.springframework.amqp.core.Queue batch3() {
			return QueueBuilder.nonDurable("c.batch.3")
					.autoDelete()
					.deadLetterExchange("")
					.deadLetterRoutingKey("c.batch.3.dlq")
					.build();
		}

		@Bean
		public org.springframework.amqp.core.Queue batch3Dlq() {
			return QueueBuilder.nonDurable("c.batch.3.dlq")
					.autoDelete()
					.build();
		}

		@Bean
		public org.springframework.amqp.core.Queue batch4() {
			return QueueBuilder.nonDurable("c.batch.4")
					.autoDelete()
					.deadLetterExchange("")
					.deadLetterRoutingKey("c.batch.4.dlq")
					.build();
		}

		@Bean
		public org.springframework.amqp.core.Queue batch4Dlq() {
			return QueueBuilder.nonDurable("c.batch.4.dlq")
					.autoDelete()
					.build();
		}

		@Bean
		public org.springframework.amqp.core.Queue batch5() {
			return QueueBuilder.nonDurable("c.batch.5")
					.autoDelete()
					.deadLetterExchange("")
					.deadLetterRoutingKey("c.batch.5.dlq")
					.build();
		}

		@Bean
		public org.springframework.amqp.core.Queue batch5Dlq() {
			return QueueBuilder.nonDurable("c.batch.5.dlq")
					.autoDelete()
					.build();
		}

	}

	public static class Listener {

		final List<Foo> foos = new ArrayList<>();

		final List<Message<Foo>> fooMessages = new ArrayList<>();

		final List<Foo> dlqd = new ArrayList<>();

		final List<Foo> dlqHalf = new ArrayList<>();

		final List<Message<Foo>> dlqOneRejectedMessages = new ArrayList<>();

		final List<Foo> dlqOneRejected = new ArrayList<>();

		CountDownLatch foosLatch = new CountDownLatch(1);

		CountDownLatch fooMessagesLatch = new CountDownLatch(1);

		CountDownLatch dlqLatch = new CountDownLatch(1);

		CountDownLatch dlqHalfLatch = new CountDownLatch(1);

		CountDownLatch dlqOneRejectedLatch1 = new CountDownLatch(1);

		CountDownLatch dlqOneRejectedLatch2 = new CountDownLatch(1);

		volatile boolean first = true;

		@RabbitListener(id = "batch.1", queues = "c.batch.1")
		public void listen1(List<Foo> in) {
			this.foos.addAll(in);
			if (this.first) {
				this.first = false;
				throw new RuntimeException();
			}
			this.foosLatch.countDown();
			this.first = true;
		}

		@RabbitListener(queues = "c.batch.2")
		public void listen2(List<Message<Foo>> in, Channel channel) throws IOException {
			this.fooMessages.addAll(in);
			if (this.first) {
				this.first = false;
				channel.basicAck(in.get(1).getHeaders().get(AmqpHeaders.DELIVERY_TAG, Long.class), true); // ack 1,2
				throw new RuntimeException();
			}
			this.fooMessagesLatch.countDown();
			this.first = true;
		}

		@RabbitListener(queues = "c.batch.3")
		public void listen3(@SuppressWarnings("unused") List<Foo> in) {
			throw new AmqpRejectAndDontRequeueException("test.batch,rejection");
		}

		@RabbitListener(queues = "c.batch.3.dlq")
		public void listen3dlq(List<Foo> in) {
			this.dlqd.addAll(in);
			this.dlqLatch.countDown();
		}

		@RabbitListener(queues = "c.batch.4")
		public void listen4(List<Message<Foo>> in, Channel channel) throws IOException {
			channel.basicAck(in.get(1).getHeaders().get(AmqpHeaders.DELIVERY_TAG, Long.class), true); // ack 1,2
			throw new AmqpRejectAndDontRequeueException("test.batch,rejection");
		}

		@RabbitListener(queues = "c.batch.4.dlq")
		public void listen4dlq(List<Foo> in) {
			this.dlqHalf.addAll(in);
			this.dlqHalfLatch.countDown();
		}

		@RabbitListener(queues = "c.batch.5")
		public void listen5(List<Message<Foo>> in, Channel channel) throws IOException {
			this.dlqOneRejectedMessages.addAll(in);
			if (this.first) {
				this.first = false;
				channel.basicReject(in.get(1).getHeaders().get(AmqpHeaders.DELIVERY_TAG, Long.class), false); // nack 2
				throw new RuntimeException();
			}
			this.dlqOneRejectedLatch1.countDown();
			this.first = true;
		}

		@RabbitListener(queues = "c.batch.5.dlq")
		public void listen5dlq(List<Foo> in) {
			this.dlqOneRejected.addAll(in);
			this.dlqOneRejectedLatch2.countDown();
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

		@Override
		public String toString() {
			return "Foo [bar=" + this.bar + "]";
		}

	}

}
