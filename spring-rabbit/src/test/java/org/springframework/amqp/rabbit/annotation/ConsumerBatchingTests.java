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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
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
 * @since 5.2
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable
public class ConsumerBatchingTests {

	@Autowired
	private RabbitTemplate template;

	@Autowired
	private Listener listener;

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
				.extracting(Foo::getBar)
				.contains("bar");
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		@Bean
		public ConnectionFactory connectionFactory() {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		}

		@Bean
		public RabbitTemplate template() {
			return new RabbitTemplate(connectionFactory());
		}

		@Bean
		public RabbitAdmin admin() {
			return new RabbitAdmin(template());
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(connectionFactory());
			SimpleMessageConverter simpleMessageConverter = new SimpleMessageConverter();
			simpleMessageConverter.addAllowedListPatterns(Foo.class.getName());
			factory.setMessageConverter(simpleMessageConverter);
			factory.setBatchListener(true);
			factory.setConsumerBatchEnabled(true);
			factory.setBatchSize(4);
			return factory;
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
		public org.springframework.amqp.core.Queue batch5() {
			return QueueBuilder.durable("c.batch.5")
					.autoDelete()
					.deadLetterExchange("")
					.deadLetterRoutingKey("c.batch.5.dlq")
					.build();
		}

		@Bean
		public org.springframework.amqp.core.Queue batch5Dlq() {
			return QueueBuilder.durable("c.batch.5.dlq")
					.autoDelete()
					.build();
		}

	}

	public static class Listener {

		final List<Message<Foo>> dlqOneRejectedMessages = new ArrayList<>();

		final List<Foo> dlqOneRejected = new ArrayList<>();

		CountDownLatch dlqOneRejectedLatch1 = new CountDownLatch(1);

		CountDownLatch dlqOneRejectedLatch2 = new CountDownLatch(1);

		volatile boolean first = true;

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
