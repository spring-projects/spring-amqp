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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LongRunningIntegrationTest;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 * @since 1.0
 *
 */
public class MessageListenerContainerLifecycleIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerContainerLifecycleIntegrationTests.class);

	private static Queue queue = new Queue("test.queue");

	private enum TransactionMode {
		ON, OFF, PREFETCH, PREFETCH_NO_TX;
		public boolean isTransactional() {
			return this != OFF && this != PREFETCH_NO_TX;
		}

		public AcknowledgeMode getAcknowledgeMode() {
			return this == OFF ? AcknowledgeMode.NONE : AcknowledgeMode.AUTO;
		}

		public int getPrefetch() {
			return this == PREFETCH || this == PREFETCH_NO_TX ? 10 : -1;
		}

		public int getTxSize() {
			return this == PREFETCH || this == PREFETCH_NO_TX ? 5 : -1;
		}
	}

	private enum Concurrency {
		LOW(1), HIGH(5);
		private final int value;

		Concurrency(int value) {
			this.value = value;
		}

		public int value() {
			return this.value;
		}
	}

	private enum MessageCount {
		LOW(1), MEDIUM(20), HIGH(500);
		private final int value;

		MessageCount(int value) {
			this.value = value;
		}

		public int value() {
			return this.value;
		}
	}

	@Rule
	public LongRunningIntegrationTest longTests = new LongRunningIntegrationTest();

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue.getName());

	@Rule
	public LogLevelAdjuster logLevels = new LogLevelAdjuster(Level.INFO, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class,
			MessageListenerContainerLifecycleIntegrationTests.class);

	private RabbitTemplate createTemplate(int concurrentConsumers) {
		RabbitTemplate template = new RabbitTemplate();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
		return template;
	}

	@After
	public void tearDown() {
		this.brokerIsRunning.removeTestQueues();
	}

	@Test
	public void testTransactionalLowLevel() throws Exception {
		doTest(MessageCount.MEDIUM, Concurrency.LOW, TransactionMode.ON);
	}

	@Test
	public void testTransactionalHighLevel() throws Exception {
		doTest(MessageCount.HIGH, Concurrency.HIGH, TransactionMode.ON);
	}

	@Test
	public void testTransactionalLowLevelWithPrefetch() throws Exception {
		doTest(MessageCount.MEDIUM, Concurrency.LOW, TransactionMode.PREFETCH);
	}

	@Test
	public void testTransactionalHighLevelWithPrefetch() throws Exception {
		doTest(MessageCount.HIGH, Concurrency.HIGH, TransactionMode.PREFETCH);
	}

	@Test
	public void testNonTransactionalLowLevel() throws Exception {
		doTest(MessageCount.MEDIUM, Concurrency.LOW, TransactionMode.OFF);
	}

	@Test
	public void testNonTransactionalHighLevel() throws Exception {
		doTest(MessageCount.HIGH, Concurrency.HIGH, TransactionMode.OFF);
	}

	@Test
	public void testNonTransactionalLowLevelWithPrefetch() throws Exception {
		doTest(MessageCount.MEDIUM, Concurrency.LOW, TransactionMode.PREFETCH_NO_TX);
	}

	@Test
	public void testNonTransactionalHighLevelWithPrefetch() throws Exception {
		doTest(MessageCount.HIGH, Concurrency.HIGH, TransactionMode.PREFETCH_NO_TX);
	}

	@Test
	public void testBadCredentials() throws Exception {
		RabbitTemplate template = createTemplate(1);
		com.rabbitmq.client.ConnectionFactory cf = new com.rabbitmq.client.ConnectionFactory();
		cf.setUsername("foo");
		final CachingConnectionFactory connectionFactory = new CachingConnectionFactory(cf);
		try {
			this.doTest(MessageCount.LOW, Concurrency.LOW, TransactionMode.OFF, template, connectionFactory);
			fail("expected exception");
		}
		catch (AmqpIllegalStateException e) {
			assertTrue("Expected FatalListenerStartupException", e.getCause() instanceof FatalListenerStartupException);
		}
		catch (Throwable t) {
			fail("expected FatalListenerStartupException:" + t.getClass() + ":" + t.getMessage());
		}
		((DisposableBean) template.getConnectionFactory()).destroy();
	}

	private void doTest(MessageCount level, Concurrency concurrency, TransactionMode transactionMode) throws Exception {
		RabbitTemplate template = createTemplate(concurrency.value);
		this.doTest(level, concurrency, transactionMode, template, template.getConnectionFactory());
	}

	/**
	 * If transactionMode is OFF, the undelivered messages will be lost (ack=NONE). If it is
	 * ON, PREFETCH, or PREFETCH_NO_TX, ack=AUTO, so we should not lose any messages.
	 */
	private void doTest(MessageCount level, Concurrency concurrency, TransactionMode transactionMode,
			RabbitTemplate template, ConnectionFactory connectionFactory) throws Exception {

		int messageCount = level.value();
		int concurrentConsumers = concurrency.value();
		boolean transactional = transactionMode.isTransactional();

		CountDownLatch latch = new CountDownLatch(messageCount);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		PojoListener listener = new PojoListener(latch);
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setAcknowledgeMode(transactionMode.getAcknowledgeMode());
		container.setChannelTransacted(transactionMode.isTransactional());
		container.setConcurrentConsumers(concurrentConsumers);

		if (transactionMode.getPrefetch() > 0) {
			container.setPrefetchCount(transactionMode.getPrefetch());
			container.setTxSize(transactionMode.getTxSize());
		}
		container.setQueueNames(queue.getName());
		container.setShutdownTimeout(30000);
		container.setReceiveTimeout(50);
		container.afterPropertiesSet();
		container.start();

		try {

			boolean waited = latch.await(50, TimeUnit.MILLISECONDS);
			logger.info("All messages received before stop: " + waited);
			if (messageCount > 1) {
				assertFalse("Expected not to receive all messages before stop", waited);
			}

			assertEquals(concurrentConsumers, container.getActiveConsumerCount());
			container.stop();
			int n = 0;
			while (n++ < 100 && container.getActiveConsumerCount() > 0) {
				Thread.sleep(100);
			}
			assertEquals(0, container.getActiveConsumerCount());
			if (!transactional) {

				int messagesReceivedAfterStop = listener.getCount();
				boolean prefetchNoTx = transactionMode == TransactionMode.PREFETCH_NO_TX;
				waited = latch.await(prefetchNoTx ? 100 : 10000, TimeUnit.MILLISECONDS);
				// AMQP-338
				logger.info("All messages received after stop: " + waited + " (" + messagesReceivedAfterStop + ")");

				if (prefetchNoTx) {
					assertFalse("Didn't expect to receive all messages after stop", waited);
				}
				else {
					assertTrue("Expect to receive all messages after stop", waited);
				}
				assertEquals("Unexpected additional messages received after stop", messagesReceivedAfterStop,
						listener.getCount());

				for (int i = 0; i < messageCount; i++) {
					template.convertAndSend(queue.getName(), i + "bar");
				}
				// Even though not transactional, we shouldn't lose messages for PREFETCH_NO_TX
				int expectedAfterRestart = transactionMode == TransactionMode.PREFETCH_NO_TX ?
						messageCount * 2 - messagesReceivedAfterStop : messageCount;
				latch = new CountDownLatch(expectedAfterRestart);
				listener.reset(latch);

			}

			int messagesReceivedBeforeStart = listener.getCount();
			container.start();
			int timeout = Math.min(1 + messageCount / (4 * concurrentConsumers), 30);

			logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
			waited = latch.await(timeout, TimeUnit.SECONDS);
			logger.info("All messages received after start: " + waited);
			assertEquals(concurrentConsumers, container.getActiveConsumerCount());
			if (transactional) {
				assertTrue("Timed out waiting for message", waited);
			}
			else {
				int count = listener.getCount();
				assertTrue("Expected additional messages received after start: " + messagesReceivedBeforeStart + ">="
						+ count, messagesReceivedBeforeStart < count);
				assertNull("Messages still available", template.receive(queue.getName()));
			}

			assertEquals(concurrentConsumers, container.getActiveConsumerCount());

		}
		finally {
			container.shutdown();
		}

		int n = 0;
		while (n++ < 100 && container.getActiveConsumerCount() > 0) {
			Thread.sleep(100);
		}
		assertEquals(0, container.getActiveConsumerCount());
		assertNull(template.receiveAndConvert(queue.getName()));

		((DisposableBean) template.getConnectionFactory()).destroy();
	}

	/*
	 * Tests that only prefetch is processed after stop().
	 */
	@Test
	public void testShutDownWithPrefetch() throws Exception {

		int messageCount = 10;
		int concurrentConsumers = 1;

		RabbitTemplate template = createTemplate(concurrentConsumers);

		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		final CountDownLatch prefetched = new CountDownLatch(1);
		final CountDownLatch awaitStart1 = new CountDownLatch(1);
		final CountDownLatch awaitStart2 = new CountDownLatch(6);
		final CountDownLatch awaitStop = new CountDownLatch(1);
		final AtomicInteger received = new AtomicInteger();
		final CountDownLatch awaitConsumeFirst = new CountDownLatch(5);
		final CountDownLatch awaitConsumeSecond = new CountDownLatch(10);
		container.setMessageListener((MessageListener) message -> {
			try {
				awaitStart1.countDown();
				prefetched.await(10, TimeUnit.SECONDS);
				awaitStart2.countDown();
				awaitStop.await(10, TimeUnit.SECONDS);
				received.incrementAndGet();
				awaitConsumeFirst.countDown();
				awaitConsumeSecond.countDown();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.setConcurrentConsumers(concurrentConsumers);

		container.setPrefetchCount(5);
		container.setQueueNames(queue.getName());
		container.afterPropertiesSet();
		container.start();

		// wait until the listener has the first message...
		assertTrue(awaitStart1.await(10, TimeUnit.SECONDS));
		// ... and the remaining 4 are queued...
		@SuppressWarnings("unchecked")
		Set<BlockingQueueConsumer> consumers = (Set<BlockingQueueConsumer>) TestUtils
				.getPropertyValue(container, "consumers");
		int n = 0;
		while (n++ < 100) {
			if (consumers.size() > 0) {
				if (TestUtils.getPropertyValue(consumers.iterator().next(), "queue", BlockingQueue.class)
						.size() > 3) {
					prefetched.countDown();
					break;
				}
			}
			Thread.sleep(100);
		}
		Executors.newSingleThreadExecutor().execute(() -> container.stop());
		n = 0;
		while (container.isActive() && n++ < 100) {
			Thread.sleep(100);
		}
		assertTrue(n < 100);

		awaitStop.countDown();

		assertTrue("awaitConsumeFirst.count=" + awaitConsumeFirst.getCount(),
				awaitConsumeFirst.await(10, TimeUnit.SECONDS));
		n = 0;
		DirectFieldAccessor dfa = new DirectFieldAccessor(container);
		while (dfa.getPropertyValue("consumers") != null && n++ < 100) {
			Thread.sleep(100);
		}
		assertTrue(n < 100);
		// make sure we stopped receiving after the prefetch was consumed
		assertEquals(5, received.get());
		assertEquals(1, awaitStart2.getCount());

		container.start();
		assertTrue(awaitStart2.await(10, TimeUnit.SECONDS));
		assertTrue("awaitConsumeSecond.count=" + awaitConsumeSecond.getCount(),
				awaitConsumeSecond.await(10, TimeUnit.SECONDS));
		container.stop();
		((DisposableBean) template.getConnectionFactory()).destroy();
	}

	@Test
	public void testSimpleMessageListenerContainerStoppedWithoutWarn() throws Exception {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setPort(BrokerTestUtils.getPort());

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		Log log = spy(TestUtils.getPropertyValue(container, "logger", Log.class));
		final CountDownLatch latch = new CountDownLatch(1);
		when(log.isDebugEnabled()).thenReturn(true);
		doAnswer(invocation -> {
			latch.countDown();
			invocation.callRealMethod();
			return null;
		}).when(log).debug(
				Mockito.contains("Consumer received Shutdown Signal, processing stopped"));
		DirectFieldAccessor dfa = new DirectFieldAccessor(container);
		dfa.setPropertyValue("logger", log);
		container.setQueues(queue);
		container.setMessageListener(new MessageListenerAdapter());
		container.afterPropertiesSet();
		container.start();

		try {
			connectionFactory.destroy();

			assertTrue(latch.await(10, TimeUnit.SECONDS));
			Mockito.verify(log).debug(
					Mockito.contains("Consumer received Shutdown Signal, processing stopped"));
			Mockito.verify(log, Mockito.never()).warn(Mockito.anyString(), Mockito.any(Throwable.class));
		}
		finally {
			container.stop();
			connectionFactory.destroy();
		}
	}

	// AMQP-496
	@Test
	public void testLongLivingConsumerStoppedProperlyAfterContextClose() throws Exception {
		ConfigurableApplicationContext applicationContext =
				new AnnotationConfigApplicationContext(LongLiveConsumerConfig.class);

		RabbitTemplate template = createTemplate(1);
		template.convertAndSend(queue.getName(), "foo");

		CountDownLatch consumerLatch = applicationContext.getBean("consumerLatch", CountDownLatch.class);
		SimpleMessageListenerContainer container = applicationContext.getBean(SimpleMessageListenerContainer.class);

		assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

		applicationContext.close();

		@SuppressWarnings("rawtypes")
		ActiveObjectCounter counter = TestUtils.getPropertyValue(container, "cancellationLock", ActiveObjectCounter.class);
		assertTrue(counter.getCount() > 0);

		int n = 0;
		while (counter.getCount() > 0 && n++ < 10) {
			Thread.sleep(500);
		}
		assertTrue(n < 10);
		((DisposableBean) template.getConnectionFactory()).destroy();
	}


	@Configuration
	static class LongLiveConsumerConfig {

		@Bean
		public CountDownLatch consumerLatch() {
			return new CountDownLatch(1);
		}

		@Bean
		public ConnectionFactory connectionFactory() {
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setHost("localhost");
			connectionFactory.setPort(BrokerTestUtils.getPort());
			return connectionFactory;
		}

		@Bean
		public SimpleMessageListenerContainer container() {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory());
			container.setQueues(queue);
			container.setMessageListener((MessageListener) message -> {
				try {
					consumerLatch().countDown();
					Thread.sleep(500);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			});
			container.setShutdownTimeout(1);
			return container;
		}

	}

	public static class PojoListener {

		private final AtomicInteger count = new AtomicInteger();

		private CountDownLatch latch;

		public PojoListener(CountDownLatch latch) {
			this.latch = latch;
		}

		public void reset(CountDownLatch latch) {
			this.latch = latch;
		}

		public void handleMessage(String value) throws Exception {
			try {
				logger.debug(value + count.getAndIncrement());
				Thread.sleep(10);
			}
			finally {
				latch.countDown();
			}
		}

		public int getCount() {
			return count.get();
		}

	}

}
