/*
 * Copyright 2018-2022 the original author or authors.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate2;
import org.springframework.amqp.rabbit.RabbitConverterFuture;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import reactor.core.publisher.Mono;

/**
 * @author Gary Russell
 * @since 2.1
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable
public class AsyncListenerTests {

	@Autowired
	private EnableRabbitConfig config;

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private AsyncRabbitTemplate2 asyncTemplate;

	@Autowired
	private Queue queue1;

	@Autowired
	private Queue queue2;

	@Autowired
	private Queue queue3;

	@Autowired
	private Queue queue4;

	@Autowired
	private Queue queue5;

	@Autowired
	private Queue queue6;

	@Autowired
	private Queue queue7;

	@Autowired
	private Listener listener;

	@Autowired
	private RabbitListenerEndpointRegistry registry;

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
		this.rabbitTemplate.convertAndSend(this.queue4.getName(), "foo");
		assertThat(listener.latch4.await(10, TimeUnit.SECONDS));
	}

	@Test
	public void testRouteToDLQ() throws InterruptedException {
		this.rabbitTemplate.convertAndSend(this.queue5.getName(), "foo");
		assertThat(this.listener.latch5.await(10, TimeUnit.SECONDS)).isTrue();
		this.rabbitTemplate.convertAndSend(this.queue6.getName(), "foo");
		assertThat(this.listener.latch6.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testOverrideDontRequeue() throws Exception {
		assertThat(this.rabbitTemplate.convertSendAndReceive(this.queue7.getName(), "foo")).isEqualTo("listen7");
	}

	@Test
	public void testAuthByProps() {
		assertThat(TestUtils.getPropertyValue(this.registry.getListenerContainer("foo"),
				"possibleAuthenticationFailureFatal", Boolean.class)).isFalse();
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
		public SimpleRabbitListenerContainerFactory dontRequeueFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setMismatchedQueuesFatal(true);
			factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
			factory.setMessageConverter(converter());
			factory.setDefaultRequeueRejected(false);
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
		public AsyncRabbitTemplate2 asyncTemplate() {
			return new AsyncRabbitTemplate2(rabbitTemplate());
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
		public Queue queue4() {
			return new AnonymousQueue();
		}

		@Bean
		public Queue queue5() {
			Map<String, Object> args = new HashMap<>();
			args.put("x-dead-letter-exchange", "");
			args.put("x-dead-letter-routing-key", queue5DLQ().getName());
			return new AnonymousQueue(args);
		}

		@Bean
		public Queue queue5DLQ() {
			return new AnonymousQueue();
		}

		@Bean
		public Queue queue6() {
			Map<String, Object> args = new HashMap<>();
			args.put("x-dead-letter-exchange", "");
			args.put("x-dead-letter-routing-key", queue6DLQ().getName());
			return new AnonymousQueue(args);
		}

		@Bean
		public Queue queue6DLQ() {
			return new AnonymousQueue();
		}

		@Bean
		public Queue queue7() {
			return new AnonymousQueue();
		}

		@Bean
		public Listener listener() {
			return new Listener();
		}

		@Bean("spring.amqp.global.properties")
		public Properties properties() {
			Properties props = new Properties();
			props.setProperty("mlc.possible.authentication.failure.fatal", "false");
			return props;
		}

	}

	@Component
	public static class Listener {

		private final AtomicBoolean fooFirst = new AtomicBoolean(true);

		private final AtomicBoolean barFirst = new AtomicBoolean(true);

		private final CountDownLatch latch4 = new CountDownLatch(1);

		private final CountDownLatch latch5 = new CountDownLatch(1);

		private final CountDownLatch latch6 = new CountDownLatch(1);

		private final AtomicBoolean first7 = new AtomicBoolean(true);

		@RabbitListener(id = "foo", queues = "#{queue1.name}")
		public CompletableFuture<String> listen1(String foo) {
			CompletableFuture<String> future = new CompletableFuture<>();
			if (fooFirst.getAndSet(false)) {
				future.completeExceptionally(new RuntimeException("Future.exception"));
			}
			else {
				future.complete(foo.toUpperCase());
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

		@RabbitListener(id = "qux", queues = "#{queue4.name}")
		public CompletableFuture<Void> listen4(@SuppressWarnings("unused") String foo) {
			CompletableFuture<Void> future = new CompletableFuture<>();
			future.complete(null);
			this.latch4.countDown();
			return future;
		}

		@RabbitListener(id = "fiz", queues = "#{queue5.name}")
		public CompletableFuture<Void> listen5(@SuppressWarnings("unused") String foo) {
			CompletableFuture<Void> future = new CompletableFuture<>();
			future.completeExceptionally(new AmqpRejectAndDontRequeueException("asyncToDLQ"));
			return future;
		}

		@RabbitListener(id = "buz", queues = "#{queue5DLQ.name}")
		public void listen5DLQ(@SuppressWarnings("unused") String foo) {
			this.latch5.countDown();
		}

		@RabbitListener(id = "fix", queues = "#{queue6.name}", containerFactory = "dontRequeueFactory")
		public CompletableFuture<Void> listen6(@SuppressWarnings("unused") String foo) {
			CompletableFuture<Void> future = new CompletableFuture<>();
			future.completeExceptionally(new IllegalStateException("asyncDefaultToDLQ"));
			return future;
		}

		@RabbitListener(id = "fox", queues = "#{queue6DLQ.name}")
		public void listen6DLQ(@SuppressWarnings("unused") String foo) {
			this.latch6.countDown();
		}

		@RabbitListener(id = "overrideFactoryRequeue", queues = "#{queue7.name}",
				containerFactory = "dontRequeueFactory")
		public CompletableFuture<String> listen7(@SuppressWarnings("unused") String foo) {
			CompletableFuture<String> future = new CompletableFuture<>();
			if (this.first7.compareAndSet(true, false)) {
				future.completeExceptionally(new ImmediateRequeueAmqpException("asyncOverrideDefaultToDLQ"));
			}
			else {
				future.complete("listen7");
			}
			return future;
		}

	}

}
