/*
 * Copyright 2002-2018 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.AbstractRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.config.StatefulRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.config.StatelessRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.amqp.rabbit.test.RepeatProcessor;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.retry.policy.MapRetryContextCache;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.annotation.Repeat;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 *
 * @since 1.0
 *
 */
public class MessageListenerContainerRetryIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerContainerRetryIntegrationTests.class);

	private static Queue queue = new Queue("test.queue");

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue.getName());

	@Rule
	public LogLevelAdjuster logLevels = new LogLevelAdjuster(Level.ERROR, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class);

	@Rule
	public LogLevelAdjuster traceLevels = new LogLevelAdjuster(Level.ERROR,
			StatefulRetryOperationsInterceptorFactoryBean.class, MessageListenerContainerRetryIntegrationTests.class);

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public RepeatProcessor repeats = new RepeatProcessor();

	private RetryTemplate retryTemplate;

	private MessageConverter messageConverter;

	private RabbitTemplate createTemplate(int concurrentConsumers) {
		RabbitTemplate template = new RabbitTemplate();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
		if (messageConverter == null) {
			SimpleMessageConverter messageConverter = new SimpleMessageConverter();
			messageConverter.setCreateMessageIds(true);
			this.messageConverter = messageConverter;
		}
		template.setMessageConverter(messageConverter);
		return template;
	}

	@After
	public void tearDown() {
		if (this.repeats.isFinalizing()) {
			this.brokerIsRunning.removeTestQueues();
		}
	}

	@Test
	public void testStatefulRetryWithAllMessagesFailing() throws Exception {

		int messageCount = 10;
		int txSize = 1;
		int failFrequency = 1;
		int concurrentConsumers = 3;
		doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	@Test
	public void testStatelessRetryWithAllMessagesFailing() throws Exception {

		int messageCount = 10;
		int txSize = 1;
		int failFrequency = 1;
		int concurrentConsumers = 3;
		doTestStatelessRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	@Test
	public void testStatefulRetryWithNoMessageIds() throws Exception {

		int messageCount = 2;
		int txSize = 1;
		int failFrequency = 1;
		int concurrentConsumers = 1;
		SimpleMessageConverter messageConverter = new SimpleMessageConverter();
		// There will be no key for these messages so they cannot be recovered...
		messageConverter.setCreateMessageIds(false);
		this.messageConverter = messageConverter;
		// Beware of context cache busting if retry policy fails...
		this.retryTemplate = new RetryTemplate();
		this.retryTemplate.setRetryContextCache(new MapRetryContextCache(1));
		// The container should have shutdown, so there are now no active consumers
		exception.expectMessage("expected:<1> but was:<0>");
		doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	@Test
	@Repeat(10)
	public void testStatefulRetryWithTxSizeAndIntermittentFailure() throws Exception {

		int messageCount = 10;
		int txSize = 4;
		int failFrequency = 3;
		int concurrentConsumers = 3;
		doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	@Test
	public void testStatefulRetryWithMoreMessages() throws Exception {

		int messageCount = 200;
		int txSize = 10;
		int failFrequency = 6;
		int concurrentConsumers = 3;
		doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	private Advice createRetryInterceptor(final CountDownLatch latch, boolean stateful) throws Exception {
		AbstractRetryOperationsInterceptorFactoryBean factory;
		if (stateful) {
			factory = new StatefulRetryOperationsInterceptorFactoryBean();
		}
		else {
			factory = new StatelessRetryOperationsInterceptorFactoryBean();
		}
		factory.setMessageRecoverer((message, cause) -> {
			logger.warn("Recovered: [" + SerializationUtils.deserialize(message.getBody()).toString() +
					"], message: " + message);
			latch.countDown();
		});
		if (retryTemplate == null) {
			retryTemplate = new RetryTemplate();
		}
		factory.setRetryOperations(retryTemplate);
		return factory.getObject();
	}

	private void doTestStatefulRetry(int messageCount, int txSize, int failFrequency, int concurrentConsumers)
			throws Exception {
		doTestRetry(messageCount, txSize, failFrequency, concurrentConsumers, true);
	}

	private void doTestStatelessRetry(int messageCount, int txSize, int failFrequency, int concurrentConsumers)
			throws Exception {
		doTestRetry(messageCount, txSize, failFrequency, concurrentConsumers, false);
	}

	private void doTestRetry(int messageCount, int txSize, int failFrequency, int concurrentConsumers, boolean stateful)
			throws Exception {

		int failedMessageCount = messageCount / failFrequency + (messageCount % failFrequency == 0 ? 0 : 1);

		RabbitTemplate template = createTemplate(concurrentConsumers);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i);
		}

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(
				template.getConnectionFactory());
		PojoListener listener = new PojoListener(failFrequency);
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.setChannelTransacted(true);
		container.setTxSize(txSize);
		container.setConcurrentConsumers(concurrentConsumers);

		final CountDownLatch latch = new CountDownLatch(failedMessageCount);
		container.setAdviceChain(new Advice[] { createRetryInterceptor(latch, stateful) });

		container.setQueueNames(queue.getName());
		container.setReceiveTimeout(50);
		container.afterPropertiesSet();
		container.start();

		try {

			int timeout = Math.min(1 + 2 * messageCount / concurrentConsumers, 30);

			final int count = messageCount;
			logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
			Executors.newSingleThreadExecutor().execute(() -> {
				while (container.getActiveConsumerCount() > 0) {
					try {
						Thread.sleep(100L);
					}
					catch (InterruptedException e) {
						latch.countDown();
						Thread.currentThread().interrupt();
						return;
					}
				}
				for (int i = 0; i < count; i++) {
					latch.countDown();
				}
			});
			boolean waited = latch.await(timeout, TimeUnit.SECONDS);
			logger.info("All messages recovered: " + waited);
			assertEquals(concurrentConsumers, container.getActiveConsumerCount());
			assertTrue("Timed out waiting for messages", waited);

			// Retried each failure 3 times (default retry policy)...
			assertEquals(3 * failedMessageCount, listener.getCount());

			// All failed messages recovered
			assertEquals(null, template.receiveAndConvert(queue.getName()));

		}
		finally {
			container.shutdown();
			((DisposableBean) template.getConnectionFactory()).destroy();

			assertEquals(0, container.getActiveConsumerCount());
		}

	}

	private static class PojoListener {

		private final AtomicInteger count = new AtomicInteger();

		private final int failFrequency;

		PojoListener(int failFrequency) {
			this.failFrequency = failFrequency;
		}

		@SuppressWarnings("unused")
		public void handleMessage(int value) throws Exception {
			logger.debug("Handling: [" + value + "], fails:" + count);
			if (value % failFrequency == 0) {
				count.getAndIncrement();
				logger.debug("Failing: [" + value + "], fails:" + count);
				throw new RuntimeException("Planned");
			}
		}

		public int getCount() {
			return count.get();
		}

	}

}
