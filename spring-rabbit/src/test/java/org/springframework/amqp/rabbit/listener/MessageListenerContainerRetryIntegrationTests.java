/*
 * Copyright 2002-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.BatchMessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.AbstractRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.config.StatefulRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.config.StatelessRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.retry.MessageBatchRecoverer;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.retry.policy.MapRetryContextCache;
import org.springframework.retry.support.RetryTemplate;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 *
 * @since 1.0
 *
 */
@RabbitAvailable(queues = MessageListenerContainerRetryIntegrationTests.TEST_QUEUE)
@LogLevels(level = "ERROR", classes = {
		RabbitTemplate.class, SimpleMessageListenerContainer.class, BlockingQueueConsumer.class,
		StatefulRetryOperationsInterceptorFactoryBean.class, MessageListenerContainerRetryIntegrationTests.class })
public class MessageListenerContainerRetryIntegrationTests {

	public static final String TEST_QUEUE = "test.queue.MessageListenerContainerRetryIntegrationTests";

	private static Log logger = LogFactory.getLog(MessageListenerContainerRetryIntegrationTests.class);

	private static Queue queue = new Queue(TEST_QUEUE);

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
			SimpleMessageConverter converter = new SimpleMessageConverter();
			converter.setCreateMessageIds(true);
			this.messageConverter = converter;
		}
		template.setMessageConverter(messageConverter);
		return template;
	}

	@Test
	void testStatelessRetryWithBatchListener() throws Exception {
		doTestRetryWithBatchListener(false);
	}

	@Test
	void testStatefulRetryWithBatchListener() throws Exception {
		doTestRetryWithBatchListener(true);
	}

	private void doTestRetryWithBatchListener(boolean stateful) throws Exception {
		RabbitTemplate template = createTemplate(1);
		template.convertAndSend(queue.getName(), "foo");
		template.convertAndSend(queue.getName(), "bar");

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(
				template.getConnectionFactory());
		container.setMessageListener((BatchMessageListener) messages -> {
			throw new RuntimeException("intended");
		});
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.setConsumerBatchEnabled(true);
		assertThat(container.isConsumerBatchEnabled()).isTrue();
		container.setBatchSize(2);

		final CountDownLatch latch = new CountDownLatch(1);
		container.setAdviceChain(createRetryInterceptor(latch, stateful, true));

		container.setQueueNames(queue.getName());
		container.setReceiveTimeout(50);
		container.afterPropertiesSet();
		container.start();

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	@Test
	void testStatefulRetryWithAllMessagesFailing() throws Exception {

		int messageCount = 10;
		int txSize = 1;
		int failFrequency = 1;
		int concurrentConsumers = 3;
		doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	@Test
	void testStatelessRetryWithAllMessagesFailing() throws Exception {

		int messageCount = 10;
		int txSize = 1;
		int failFrequency = 1;
		int concurrentConsumers = 3;
		doTestStatelessRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	@Test
	void testStatefulRetryWithNoMessageIds() throws Exception {

		int messageCount = 2;
		int txSize = 1;
		int failFrequency = 1;
		int concurrentConsumers = 1;
		SimpleMessageConverter converter = new SimpleMessageConverter();
		// There will be no key for these messages so they cannot be recovered...
		converter.setCreateMessageIds(false);
		this.messageConverter = converter;
		// Beware of context cache busting if retry policy fails...
		this.retryTemplate = new RetryTemplate();
		this.retryTemplate.setRetryContextCache(new MapRetryContextCache(1));
		// The container should have shutdown, so there are now no active consumers
		assertThatThrownBy(() -> doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers))
			.hasMessageContaining("expected: 1")
			.hasMessageContaining("but was: 0");
	}

	@RepeatedTest(10)
	void testStatefulRetryWithTxSizeAndIntermittentFailure() throws Exception {

		int messageCount = 10;
		int txSize = 4;
		int failFrequency = 3;
		int concurrentConsumers = 3;
		doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	@Test
	void testStatefulRetryWithMoreMessages() throws Exception {

		int messageCount = 200;
		int txSize = 10;
		int failFrequency = 6;
		int concurrentConsumers = 3;
		doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	private Advice createRetryInterceptor(final CountDownLatch latch, boolean stateful) throws Exception {
		return createRetryInterceptor(latch, stateful, false);
	}

	private Advice createRetryInterceptor(final CountDownLatch latch, boolean stateful, boolean listRecoverer)
			throws Exception {

		AbstractRetryOperationsInterceptorFactoryBean factory;
		if (stateful) {
			factory = new StatefulRetryOperationsInterceptorFactoryBean();
		}
		else {
			factory = new StatelessRetryOperationsInterceptorFactoryBean();
		}
		if (listRecoverer) {
			factory.setMessageRecoverer((MessageBatchRecoverer) (messages, cause) -> {
				latch.countDown();
			});
		}
		else {
			factory.setMessageRecoverer((message, cause) -> {
				latch.countDown();
			});
		}
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
		container.setBatchSize(txSize);
		container.setConcurrentConsumers(concurrentConsumers);

		final CountDownLatch latch = new CountDownLatch(failedMessageCount);
		container.setAdviceChain(createRetryInterceptor(latch, stateful));

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
			assertThat(container.getActiveConsumerCount()).isEqualTo(concurrentConsumers);
			assertThat(waited).as("Timed out waiting for messages").isTrue();

			// Retried each failure 3 times (default retry policy)...
			assertThat(listener.getCount()).isEqualTo(3 * failedMessageCount);

			// All failed messages recovered
			assertThat(template.receiveAndConvert(queue.getName())).isEqualTo(null);

		}
		finally {
			container.shutdown();
			((DisposableBean) template.getConnectionFactory()).destroy();

			assertThat(container.getActiveConsumerCount()).isEqualTo(0);
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
