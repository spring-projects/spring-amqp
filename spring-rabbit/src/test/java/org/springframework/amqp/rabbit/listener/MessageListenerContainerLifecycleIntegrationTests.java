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

package org.springframework.amqp.rabbit.listener;

import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.rabbitmq.client.DnsRecordIpAddressResolver;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.LongRunning;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException;
import org.springframework.amqp.rabbit.support.ActiveObjectCounter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.DisabledIf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.spy;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 *
 * @since 1.0
 *
 */
@RabbitAvailable(queues = MessageListenerContainerLifecycleIntegrationTests.TEST_QUEUE)
@LongRunning
@LogLevels(classes = {RabbitTemplate.class,
		SimpleMessageListenerContainer.class, BlockingQueueConsumer.class,
		MessageListenerContainerLifecycleIntegrationTests.class}, level = "INFO")
public class MessageListenerContainerLifecycleIntegrationTests {

	public static final String TEST_QUEUE = "test.queue.MessageListenerContainerLifecycleIntegrationTests";

	private static final Log logger = LogFactory.getLog(MessageListenerContainerLifecycleIntegrationTests.class);

	private static final Queue queue = new Queue(TEST_QUEUE);

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

	private RabbitTemplate createTemplate(int concurrentConsumers) {
		RabbitTemplate template = new RabbitTemplate();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
		return template;
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

	/**
	 * If localhost also resolves to an IPv6 address the client will try that
	 * after a failure due to bad credentials and, if Rabbit is not listening there
	 * we won't get a fatal startup exception because a connect exception is not
	 * considered fatal.
	 * @throws UnknownHostException unknown host
	 */
	public static boolean checkIpV6() throws UnknownHostException {
		DnsRecordIpAddressResolver resolver = new DnsRecordIpAddressResolver("localhost");
		return resolver.getAddresses().size() > 1;
	}

	@Test
	@DisabledIf("#{T(org.springframework.amqp.rabbit.listener.MessageListenerContainerLifecycleIntegrationTests)"
			+ ".checkIpV6()}")
	public void testBadCredentials() throws Exception {
		RabbitTemplate template = createTemplate(1);
		com.rabbitmq.client.ConnectionFactory cf = new com.rabbitmq.client.ConnectionFactory();
		cf.setAutomaticRecoveryEnabled(false);
		cf.setUsername("foo");
		final CachingConnectionFactory connectionFactory = new CachingConnectionFactory(cf);
		assertThatExceptionOfType(AmqpIllegalStateException.class).isThrownBy(() ->
						doTest(MessageCount.LOW, Concurrency.LOW, TransactionMode.OFF, template, connectionFactory))
				.withCauseExactlyInstanceOf(FatalListenerStartupException.class);
		((DisposableBean) template.getConnectionFactory()).destroy();
	}

	private void doTest(MessageCount level, Concurrency concurrency, TransactionMode transactionMode) throws Exception {
		RabbitTemplate template = createTemplate(concurrency.value);
		doTest(level, concurrency, transactionMode, template, template.getConnectionFactory());
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
			container.setBatchSize(transactionMode.getTxSize());
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
				assertThat(waited).as("Expected not to receive all messages before stop").isFalse();
			}

			assertThat(container.getActiveConsumerCount()).isEqualTo(concurrentConsumers);
			container.stop();
			await().until(() -> container.getActiveConsumerCount() == 0);
			if (!transactional) {

				int messagesReceivedAfterStop = listener.getCount();
				boolean prefetchNoTx = transactionMode == TransactionMode.PREFETCH_NO_TX;
				waited = latch.await(prefetchNoTx ? 100 : 10000, TimeUnit.MILLISECONDS);
				// AMQP-338
				logger.info("All messages received after stop: " + waited + " (" + messagesReceivedAfterStop + ")");

				if (prefetchNoTx) {
					assertThat(waited).as("Didn't expect to receive all messages after stop").isFalse();
				}
				else {
					assertThat(waited).as("Expect to receive all messages after stop").isTrue();
				}
				assertThat(listener.getCount()).as("Unexpected additional messages received after stop").isEqualTo(messagesReceivedAfterStop);

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
			assertThat(container.getActiveConsumerCount()).isEqualTo(concurrentConsumers);
			if (transactional) {
				assertThat(waited).as("Timed out waiting for message").isTrue();
			}
			else {
				int count = listener.getCount();
				assertThat(messagesReceivedBeforeStart < count).as("Expected additional messages received after start: " + messagesReceivedBeforeStart + ">="
						+ count).isTrue();
				assertThat(template.receive(queue.getName())).as("Messages still available").isNull();
			}

			assertThat(container.getActiveConsumerCount()).isEqualTo(concurrentConsumers);

		}
		finally {
			container.shutdown();
		}

		await().until(() -> container.getActiveConsumerCount() == 0);
		assertThat(template.receiveAndConvert(queue.getName())).isNull();

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
		container.setMessageListener(message -> {
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
		container.setReceiveTimeout(10);
		container.afterPropertiesSet();
		container.start();

		// wait until the listener has the first message...
		assertThat(awaitStart1.await(10, TimeUnit.SECONDS)).isTrue();
		// ... and the remaining 4 are queued...
		@SuppressWarnings("unchecked")
		Set<BlockingQueueConsumer> consumers = (Set<BlockingQueueConsumer>) TestUtils
				.getPropertyValue(container, "consumers");
		await().until(() -> {
			if (!consumers.isEmpty()
					&& TestUtils.<BlockingQueue<?>>propertyValue(consumers.iterator().next(), "queue").size() > 3) {
				prefetched.countDown();
				return true;
			}
			else {
				return false;
			}
		});
		Executors.newSingleThreadExecutor().execute(container::stop);
		await().until(() -> !container.isActive());
		awaitStop.countDown();

		assertThat(awaitConsumeFirst.await(10, TimeUnit.SECONDS))
				.as("awaitConsumeFirst.count=" + awaitConsumeFirst.getCount()).isTrue();
		DirectFieldAccessor dfa = new DirectFieldAccessor(container);
		await().until(() -> dfa.getPropertyValue("consumers") == null);
		// make sure we stopped receiving after the prefetch was consumed
		assertThat(received.get()).isEqualTo(5);
		assertThat(awaitStart2.getCount()).isEqualTo(1);

		container.start();
		assertThat(awaitStart2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(awaitConsumeSecond.await(10, TimeUnit.SECONDS))
				.as("awaitConsumeSecond.count=" + awaitConsumeSecond.getCount()).isTrue();
		container.stop();
		((DisposableBean) template.getConnectionFactory()).destroy();
	}

	@Test
	public void testSimpleMessageListenerContainerStoppedWithoutWarn() throws Exception {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setPort(BrokerTestUtils.getPort());

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		Log log = spy(TestUtils.<Log>propertyValue(container, "logger"));
		final CountDownLatch latch = new CountDownLatch(1);
		given(log.isDebugEnabled()).willReturn(true);
		willAnswer(invocation -> {
			latch.countDown();
			invocation.callRealMethod();
			return null;
		}).given(log).debug(
				Mockito.contains("Consumer received Shutdown Signal, processing stopped"));
		DirectFieldAccessor dfa = new DirectFieldAccessor(container);
		dfa.setPropertyValue("logger", log);
		container.setQueues(queue);
		container.setReceiveTimeout(10);
		container.setMessageListener(new MessageListenerAdapter());
		container.afterPropertiesSet();
		container.start();

		try {
			connectionFactory.destroy();

			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
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

		assertThat(consumerLatch.await(10, TimeUnit.SECONDS)).isTrue();

		applicationContext.close();

		ActiveObjectCounter<?> counter = TestUtils.propertyValue(container, "cancellationLock");
		assertThat(counter.getCount()).isGreaterThan(0);

		await().until(() -> counter.getCount() == 0);
		((DisposableBean) template.getConnectionFactory()).destroy();
	}

	@Test
	public void testConcurrencyConfiguration() {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setConcurrentConsumers(1);
		container.setMaxConcurrentConsumers(1);
		container.setConcurrency("2-5");

		assertThat(TestUtils.getPropertyValue(container, "concurrentConsumers")).isEqualTo(2);
		assertThat(TestUtils.getPropertyValue(container, "maxConcurrentConsumers")).isEqualTo(5);
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
			container.setReceiveTimeout(10);
			container.setMessageListener(message -> {
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
