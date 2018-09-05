/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.annotation;

import static org.junit.Assert.assertEquals;

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
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private AsyncRabbitTemplate asyncTemplate;

	@Autowired
	private Queue queue1;

	@Autowired
	private Queue queue2;

	@Test
	public void testAsyncListener() throws Exception {
		assertEquals("FOO", this.rabbitTemplate.convertSendAndReceive(this.queue1.getName(), "foo"));
		RabbitConverterFuture<Object> future = this.asyncTemplate.convertSendAndReceive(this.queue1.getName(), "foo");
		assertEquals("FOO", future.get(10, TimeUnit.SECONDS));
		assertEquals("FOO", this.rabbitTemplate.convertSendAndReceive(this.queue2.getName(), "foo"));
	}

	@Configuration
	@EnableRabbit
	public static class EnableRabbitConfig {

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setMismatchedQueuesFatal(true);
			factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
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
			return new RabbitTemplate(rabbitConnectionFactory());
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
		public Mono<String> listen2(String foo) {
			if (barFirst.getAndSet(false)) {
				return Mono.error(new RuntimeException("Mono.error()"));
			}
			else {
				return Mono.just(foo.toUpperCase());
			}
		}

	}

}
