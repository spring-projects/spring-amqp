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

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
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
 * @author Kai Stapel
 * @since 2.2
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable(queues = { "json.batch.1", "json.batch.2" })
public class EnableRabbitBatchJsonIntegrationTests {

	@Autowired
	private BatchingRabbitTemplate template;

	@Autowired
	private Listener listener;

	@Test
	public void testSimpleList() throws Exception {
		this.template.send("json.batch.1", msg("{\"bar\":\"foo\"}"));
		this.template.send("json.batch.1", msg("{\"bar\":\"bar\"}"));
		assertThat(this.listener.foosLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.foos.get(0).getBar()).isEqualTo("foo");
		assertThat(this.listener.foos.get(1).getBar()).isEqualTo("bar");
	}

	@Test
	public void testMessageList() throws Exception {
		this.template.send("json.batch.2", msg("{\"bar\":\"foo\"}"));
		this.template.send("json.batch.2", msg("{\"bar\":\"bar\"}"));
		assertThat(this.listener.fooMessagesLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.fooMessages.get(0).getPayload().getBar()).isEqualTo("foo");
		assertThat(this.listener.fooMessages.get(1).getPayload().getBar()).isEqualTo("bar");
	}

	private org.springframework.amqp.core.Message msg(String body) throws UnsupportedEncodingException {
		MessageProperties properties = new MessageProperties();
		properties.setContentType("application/json");
		return new org.springframework.amqp.core.Message(body.getBytes(SimpleMessageConverter.DEFAULT_CHARSET), properties);
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(connectionFactory());
			factory.setBatchListener(true);
			factory.setConsumerBatchEnabled(true);
			factory.setBatchSize(2);
			factory.setMessageConverter(converter());
			return factory;
		}

		@Bean
		public BatchingRabbitTemplate template() {
			BatchingRabbitTemplate batchTemplate = new BatchingRabbitTemplate(connectionFactory(),
					new SimpleBatchingStrategy(2, 10_000, 10_000L), scheduler());
			batchTemplate.setMessageConverter(converter());
			return batchTemplate;
		}

		@Bean
		public Jackson2JsonMessageConverter converter() {
			return new Jackson2JsonMessageConverter();
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

		@RabbitListener(queues = "json.batch.1")
		public void listen1(List<Foo> in) {
			this.foos = in;
			this.foosLatch.countDown();
		}

		@RabbitListener(queues = "json.batch.2")
		public void listen2(List<Message<Foo>> in) {
			this.fooMessages = in;
			this.fooMessagesLatch.countDown();
		}

	}

	public static class Foo {

		private String bar;

		public Foo() {
		}

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
