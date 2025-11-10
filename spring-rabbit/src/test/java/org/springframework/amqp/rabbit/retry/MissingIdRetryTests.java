/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.rabbit.retry;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.aopalliance.aop.Advice;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.StatefulRetryOperationsInterceptor;
import org.springframework.amqp.rabbit.config.StatefulRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.rabbit.listener.BlockingQueueConsumer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Gary Russell
 * @author Arnaud Cogoluègnes
 * @author Stephane Nicoll
 *
 * @since 1.1.2
 */
@RabbitAvailable
@LogLevels(classes = { BlockingQueueConsumer.class,
		MissingIdRetryTests.class,
		RetryTemplate.class })
public class MissingIdRetryTests {

	private volatile CountDownLatch latch;

	@BeforeAll
	@AfterAll
	public static void setupAndCleanUp() {
		RabbitAvailableCondition.getBrokerRunning().deleteQueues("retry.test.queue");
		RabbitAvailableCondition.getBrokerRunning().deleteExchanges("retry.test.exchange");
	}

	@Test
	public void testWithNoId() throws Exception {
		// 2 messages; each retried once by missing id interceptor
		this.latch = new CountDownLatch(4);
		ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext("retry-context.xml", this.getClass());
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setStatefulRetryFatalWithNullMessageId(false);
		container.setMessageListener(new MessageListenerAdapter(new POJO()));
		container.setQueueNames("retry.test.queue");
		container.setReceiveTimeout(10);

		StatefulRetryOperationsInterceptorFactoryBean fb = new StatefulRetryOperationsInterceptorFactoryBean();

		Advice retryInterceptor = fb.getObject();
		container.setAdviceChain(retryInterceptor);
		container.start();
		Map<String, Object> cache = getCache(retryInterceptor);

		template.convertAndSend("retry.test.exchange", "retry.test.binding", "Hello, world!");
		template.convertAndSend("retry.test.exchange", "retry.test.binding", "Hello, world!");
		try {
			assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(cache).isEmpty();
		}
		finally {
			container.stop();
			ctx.close();
		}
	}

	@Test
	public void testWithId() throws Exception {
		// 2 messages; each retried twice by retry interceptor
		RetryPolicy retryPolicy = RetryPolicy.builder().maxRetries(2).delay(Duration.ZERO).build();
		this.latch = new CountDownLatch(6);
		ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext("retry-context.xml", this.getClass());
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setPrefetchCount(1);
		container.setReceiveTimeout(10);
		container.setMessageListener(new MessageListenerAdapter(new POJO()));
		container.setQueueNames("retry.test.queue");

		StatefulRetryOperationsInterceptorFactoryBean fb = new StatefulRetryOperationsInterceptorFactoryBean();
		fb.setRetryPolicy(retryPolicy);
		fb.setMessageRecoverer(new RejectAndDontRequeueRecoverer(() -> "Don't requeue"));

		Advice retryInterceptor = fb.getObject();
		container.setAdviceChain(retryInterceptor);
		container.start();
		Map<String, Object> cache = getCache(retryInterceptor);

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("text/plain");
		messageProperties.setMessageId("foo");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		template.send("retry.test.exchange", "retry.test.binding", message);
		template.send("retry.test.exchange", "retry.test.binding", message);
		try {
			assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
			await().atMost(Duration.ofSeconds(3)).until(cache::isEmpty);
		}
		finally {
			container.stop();
			ctx.close();
		}
	}

	@Test
	public void testWithIdAndSuccess() throws Exception {
		RetryPolicy retryPolicy = RetryPolicy.builder().maxRetries(2).delay(Duration.ZERO).build();
		// 2 messages; each retried twice by retry interceptor
		this.latch = new CountDownLatch(6);
		ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext("retry-context.xml", this.getClass());
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		final Set<@Nullable String> processed = new HashSet<>();
		final CountDownLatch cdl = new CountDownLatch(4);
		container.setMessageListener(m -> {
			cdl.countDown();
			if (!processed.contains(m.getMessageProperties().getMessageId())) {
				processed.add(m.getMessageProperties().getMessageId());
				throw new RuntimeException("fail");
			}
		});
		container.setQueueNames("retry.test.queue");
		container.setReceiveTimeout(10);

		StatefulRetryOperationsInterceptorFactoryBean fb = new StatefulRetryOperationsInterceptorFactoryBean();
		fb.setRetryPolicy(retryPolicy);
		fb.setMessageRecoverer(new RejectAndDontRequeueRecoverer());

		StatefulRetryOperationsInterceptor retryInterceptor = fb.getObject();
		container.setAdviceChain(retryInterceptor);
		container.start();
		Map<String, Object> cache = getCache(retryInterceptor);

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("text/plain");
		messageProperties.setMessageId("foo");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		template.send("retry.test.exchange", "retry.test.binding", message);
		messageProperties.setMessageId("bar");
		template.send("retry.test.exchange", "retry.test.binding", message);
		try {
			assertThat(cdl.await(30, TimeUnit.SECONDS)).isTrue();
			await().atMost(Duration.ofSeconds(3)).until(cache::isEmpty);
		}
		finally {
			container.stop();
			ctx.close();
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> getCache(Advice retryInterceptor) {
		return (Map<String, Object>) Objects.requireNonNull(ReflectionTestUtils.getField(retryInterceptor, "retryStateCache"));
	}

	public class POJO {

		public void handleMessage(String foo) {
			latch.countDown();
			throw new RuntimeException("fail");
		}

	}

}
