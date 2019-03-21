/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitConverterFuture;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import reactor.core.publisher.Mono;

/**
 * @author Gary Russell
 * @since 2.1
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class AsyncListenerTests {

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunning();

	@Autowired
	private EnableRabbitConfig config;

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private AsyncRabbitTemplate asyncTemplate;

	@Autowired
	private Queue queue1;

	@Autowired
	private Queue queue2;

	@Autowired
	private Queue queue3;

	@Test
	public void testAsyncListener() throws Exception {
		assertThat(this.rabbitTemplate.convertSendAndReceive(this.queue1.getName(), "foo")).isEqualTo("FOO");
		RabbitConverterFuture<Object> future = this.asyncTemplate.convertSendAndReceive(this.queue1.getName(), "foo");
		assertThat(future.get(10, TimeUnit.SECONDS)).isEqualTo("FOO");
		assertThat(this.config.typeId).isEqualTo("java.lang.String");
		assertThat(this.rabbitTemplate.convertSendAndReceive(this.queue2.getName(), "foo")).isEqualTo("FOO");
		assertThat(this.config.typeId).isEqualTo("java.lang.String");
		List<String> foos = new ArrayList<>();
		foos.add("FOO");
		assertThat(this.rabbitTemplate.convertSendAndReceive(this.queue3.getName(), "foo")).isEqualTo(foos);
		assertThat(this.config.typeId).isEqualTo("java.util.List");
		assertThat(this.config.contentTypeId).isEqualTo("java.lang.String");
	}

	@Configuration
	@EnableRabbit
	public static class EnableRabbitConfig {

		private volatile Object typeId;

		private volatile Object contentTypeId;

		@Bean
		public MessageConverter converter() {
			return new Jackson2JsonMessageConverter();
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setMismatchedQueuesFatal(true);
			factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
			factory.setMessageConverter(converter());
			return factory;
		}

		@Bean
		public ConnectionFactory rabbitConnectionFactory() {
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setHost("localhost");
			return connectionFactory;
		}

		@Bean
		public RabbitTemplate rabbitTemplate() {
			RabbitTemplate template = new RabbitTemplate(rabbitConnectionFactory());
			template.setMessageConverter(converter());
			template.setAfterReceivePostProcessors(m -> {
				this.typeId = m.getMessageProperties().getHeaders().get("__TypeId__");
				this.contentTypeId = m.getMessageProperties().getHeaders().get("__ContentTypeId__");
				return m;
			});
			return template;
		}

		@Bean
		public AsyncRabbitTemplate asyncTemplate() {
			return new AsyncRabbitTemplate(rabbitTemplate());
		}

		@Bean
		public RabbitAdmin rabbitAdmin() {
			return new RabbitAdmin(rabbitConnectionFactory());
		}

		@Bean
		public Queue queue1() {
			return new AnonymousQueue();
		}

		@Bean
		public Queue queue2() {
			return new AnonymousQueue();
		}

		@Bean
		public Queue queue3() {
			return new AnonymousQueue();
		}

		@Bean
		public Listener listener() {
			return new Listener();
		}

	}

	@Component
	public static class Listener {

		private final AtomicBoolean fooFirst = new AtomicBoolean(true);

		private final AtomicBoolean barFirst = new AtomicBoolean(true);

		@RabbitListener(id = "foo", queues = "#{queue1.name}")
		public ListenableFuture<String> listen1(String foo) {
			SettableListenableFuture<String> future = new SettableListenableFuture<>();
			if (fooFirst.getAndSet(false)) {
				future.setException(new RuntimeException("Future.exception"));
			}
			else {
				future.set(foo.toUpperCase());
			}
			return future;
		}

		@RabbitListener(id = "bar", queues = "#{queue2.name}")
		public Mono<?> listen2(String foo) {
			if (barFirst.getAndSet(false)) {
				return Mono.error(new RuntimeException("Mono.error()"));
			}
			else {
				return Mono.just(foo.toUpperCase());
			}
		}

		@RabbitListener(id = "baz", queues = "#{queue3.name}")
		public Mono<List<String>> listen3(String foo) {
			return Mono.just(Collections.singletonList(foo.toUpperCase()));
		}

	}

}
