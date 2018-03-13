/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.connection.ShutDownChannelListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Gary Russell
 * @since 1.6
 *
 */
public class ContainerInitializationTests {

	private static final String TEST_MISMATCH = "test.mismatch";

	private static final String TEST_MISMATCH2 = "test.mismatch2";

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunningWithEmptyQueues(TEST_MISMATCH, TEST_MISMATCH2);

	@After
	public void tearDown() {
		brokerRunning.removeTestQueues();
	}

	@Test
	public void testNoAdmin() throws Exception {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config0.class);
			context.close();
			fail("expected initialization failure");
		}
		catch (ApplicationContextException e) {
			assertThat(e.getCause().getCause(), instanceOf(IllegalStateException.class));
			assertThat(e.getMessage(), containsString("When 'mismatchedQueuesFatal' is 'true', there must be "
					+ "exactly one AmqpAdmin in the context or you must inject one into this container; found: 0"));
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
			assertThat(e.getCause(), instanceOf(FatalListenerStartupException.class));
		}
	}

	@Test
	public void testMismatchedQueueDuringRestart() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config2.class);
		CountDownLatch[] latches = setUpChannelLatches(context);
		RabbitAdmin admin = context.getBean(RabbitAdmin.class);
		admin.deleteQueue(TEST_MISMATCH);
		assertTrue(latches[0].await(20, TimeUnit.SECONDS));
		admin.declareQueue(new Queue(TEST_MISMATCH, false, false, true));
		latches[2].countDown(); // let container thread continue to enable restart
		assertTrue(latches[1].await(20, TimeUnit.SECONDS));
		SimpleMessageListenerContainer container = context.getBean(SimpleMessageListenerContainer.class);
		int n = 0;
		while (n++ < 200 && container.isRunning()) {
			Thread.sleep(100);
		}
		assertFalse(container.isRunning());
		context.close();
	}

	@Test
	public void testMismatchedQueueDuringRestartMultiQueue() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config3.class);
		CountDownLatch[] latches = setUpChannelLatches(context);
		RabbitAdmin admin = context.getBean(RabbitAdmin.class);
		admin.deleteQueue(TEST_MISMATCH);
		assertTrue(latches[0].await(20, TimeUnit.SECONDS));
		admin.declareQueue(new Queue(TEST_MISMATCH, false, false, true));
		latches[2].countDown(); // let container thread continue to enable restart
		assertTrue(latches[1].await(20, TimeUnit.SECONDS));
		SimpleMessageListenerContainer container = context.getBean(SimpleMessageListenerContainer.class);
		int n = 0;
		while (n++ < 200 && container.isRunning()) {
			Thread.sleep(100);
		}
		assertFalse(container.isRunning());
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
			return new RabbitAdmin(connectionFactory());
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
