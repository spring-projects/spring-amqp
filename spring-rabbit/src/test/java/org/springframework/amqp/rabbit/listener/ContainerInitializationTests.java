/*
 * Copyright 2016-2025 the original author or authors.
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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.connection.ShutDownChannelListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

/**
 * @author Gary Russell
 * @since 1.6
 *
 */
@RabbitAvailable(queues = { ContainerInitializationTests.TEST_MISMATCH, ContainerInitializationTests.TEST_MISMATCH2 })
public class ContainerInitializationTests {

	public static final String TEST_MISMATCH = "test.mismatch";

	public static final String TEST_MISMATCH2 = "test.mismatch2";

	@Test
	public void testNoAdmin() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config0.class);
			context.close();
			fail("expected initialization failure");
		}
		catch (ApplicationContextException e) {
			assertThat(e.getCause().getCause()).isInstanceOf(IllegalStateException.class);
			assertThat(e.getCause().getMessage()).contains("When 'mismatchedQueuesFatal' is 'true', there must be "
					+ "exactly one AmqpAdmin in the context or you must inject one into this container; found: 0");
		}
	}

	@Test
	public void testMismatchedQueue() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config1.class);
			context.close();
			fail("expected initialization failure");
		}
		catch (ApplicationContextException e) {
			assertThat(e.getCause()).isInstanceOf(FatalListenerStartupException.class);
		}
	}

	@Test
	public void testMismatchedQueueDuringRestart() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config2.class);
		CountDownLatch[] latches = setUpChannelLatches(context);
		RabbitAdmin admin = context.getBean(RabbitAdmin.class);
		admin.deleteQueue(TEST_MISMATCH);
		assertThat(latches[0].await(20, TimeUnit.SECONDS)).isTrue();
		admin.declareQueue(new Queue(TEST_MISMATCH, false, false, true));
		latches[2].countDown(); // let container thread continue to enable restart
		assertThat(latches[1].await(20, TimeUnit.SECONDS)).isTrue();
		SimpleMessageListenerContainer container = context.getBean(SimpleMessageListenerContainer.class);
		await().atMost(Duration.ofSeconds(20)).until(() -> !container.isRunning());
		context.close();
	}

	@Test
	public void testMismatchedQueueDuringRestartMultiQueue() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config3.class);
		CountDownLatch[] latches = setUpChannelLatches(context);
		RabbitAdmin admin = context.getBean(RabbitAdmin.class);
		admin.deleteQueue(TEST_MISMATCH);
		assertThat(latches[0].await(20, TimeUnit.SECONDS)).isTrue();
		admin.declareQueue(new Queue(TEST_MISMATCH, false, false, true));
		latches[2].countDown(); // let container thread continue to enable restart
		assertThat(latches[1].await(20, TimeUnit.SECONDS)).isTrue();
		SimpleMessageListenerContainer container = context.getBean(SimpleMessageListenerContainer.class);
		await().atMost(Duration.ofSeconds(20)).until(() -> !container.isRunning());
		context.close();
	}

	private CountDownLatch[] setUpChannelLatches(ApplicationContext context) {
		CachingConnectionFactory cf = context.getBean(CachingConnectionFactory.class);
		final CountDownLatch cancelLatch = new CountDownLatch(1);
		final CountDownLatch mismatchLatch = new CountDownLatch(1);
		final CountDownLatch preventContainerRedeclareQueueLatch = new CountDownLatch(1);
		cf.addChannelListener((ShutDownChannelListener) s -> {
			if (RabbitUtils.isNormalChannelClose(s)) {
				cancelLatch.countDown();
				try {
					preventContainerRedeclareQueueLatch.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			else if (RabbitUtils.isMismatchedQueueArgs(s)) {
				mismatchLatch.countDown();
			}
		});
		return new CountDownLatch[] { cancelLatch, mismatchLatch, preventContainerRedeclareQueueLatch };
	}

	@Configuration
	static class Config0 {

		@Bean
		public ConnectionFactory connectionFactory() {
			return new CachingConnectionFactory("localhost");
		}

		@Bean
		public SimpleMessageListenerContainer container() {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory());
			container.setQueueNames(TEST_MISMATCH);
			container.setReceiveTimeout(10);
			container.setMessageListener(new MessageListenerAdapter(new Object() {

				@SuppressWarnings("unused")
				public void handleMessage(Message m) {
				}

			}));
			container.setMismatchedQueuesFatal(true);
			return container;
		}

		@Bean
		public Queue queue() {
			return new Queue(TEST_MISMATCH, false, false, true); // mismatched
		}

	}

	@Configuration
	static class Config1 extends Config0 {

		@Bean
		public RabbitAdmin admin() {
			RabbitAdmin admin = new RabbitAdmin(connectionFactory());
			admin.setRetryTemplate(null);
			return admin;
		}

	}

	@Configuration
	static class Config2 extends Config1 {

		@Override
		@Bean
		public Queue queue() {
			return new Queue(TEST_MISMATCH, true, false, false);
		}

	}

	@Configuration
	static class Config3 extends Config2 {


		@Override
		public SimpleMessageListenerContainer container() {
			SimpleMessageListenerContainer container = super.container();
			container.setQueueNames(TEST_MISMATCH, TEST_MISMATCH2);
			return container;
		}

		@Bean
		public Queue queue2() {
			return new Queue(TEST_MISMATCH2, true, false, false);
		}

	}

}
