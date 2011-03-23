package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;

public class MessageListenerContainerLifecycleIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerContainerLifecycleIntegrationTests.class);

	private static Queue queue = new Queue("test.queue");

	private enum TransactionMode {
		ON, OFF, PREFETCH;
		public boolean isTransactional() {
			return this != OFF;
		}

		public AcknowledgeMode getAcknowledgeMode() {
			return this == OFF ? AcknowledgeMode.NONE : AcknowledgeMode.AUTO;
		}

		public int getPrefetch() {
			return this == PREFETCH ? 10 : -1;
		}

		public int getTxSize() {
			return this == PREFETCH ? 5 : -1;
		}
	}

	private enum Concurrency {
		LOW(1), HIGH(5);
		private final int value;

		private Concurrency(int value) {
			this.value = value;
		}

		public int value() {
			return this.value;
		}
	}

	private enum MessageCount {
		LOW(1), MEDIUM(20), HIGH(500);
		private final int value;

		private MessageCount(int value) {
			this.value = value;
		}

		public int value() {
			return this.value;
		}
	}

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueue(queue);

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.INFO, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class,
			MessageListenerContainerLifecycleIntegrationTests.class);

	private RabbitTemplate createTemplate(int concurrentConsumers) {
		RabbitTemplate template = new RabbitTemplate();
		// SingleConnectionFactory connectionFactory = new SingleConnectionFactory();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
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

	private void doTest(MessageCount level, Concurrency concurrency, TransactionMode transactionMode) throws Exception {

		int messageCount = level.value();
		int concurrentConsumers = concurrency.value();
		boolean transactional = transactionMode.isTransactional();

		RabbitTemplate template = createTemplate(concurrentConsumers);

		CountDownLatch latch = new CountDownLatch(messageCount);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
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
			Thread.sleep(500L);
			assertEquals(0, container.getActiveConsumerCount());
			if (!transactional) {

				int messagesReceivedAfterStop = listener.getCount();
				waited = latch.await(500, TimeUnit.MILLISECONDS);
				logger.info("All messages received after stop: " + waited);
				if (messageCount < 100) {
					assertTrue("Expected to receive all messages after stop", waited);
				}
				assertEquals("Unexpected additional messages received after stop", messagesReceivedAfterStop,
						listener.getCount());

				for (int i = 0; i < messageCount; i++) {
					template.convertAndSend(queue.getName(), i + "bar");
				}
				latch = new CountDownLatch(messageCount);
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
			} else {
				int count = listener.getCount();
				assertTrue("Expected additional messages received after start: " + messagesReceivedBeforeStart + ">="
						+ count, messagesReceivedBeforeStart < count);
				assertNull("Messages still available", template.receive(queue.getName()));
			}

			assertEquals(concurrentConsumers, container.getActiveConsumerCount());

		} finally {
			// Wait for broker communication to finish before trying to stop
			// container
			Thread.sleep(500L);
			container.shutdown();
			assertEquals(0, container.getActiveConsumerCount());
		}

		assertNull(template.receiveAndConvert(queue.getName()));

	}

	public static class PojoListener {
		private AtomicInteger count = new AtomicInteger();

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
				Thread.sleep(100L);
			} finally {
				latch.countDown();
			}
		}

		public int getCount() {
			return count.get();
		}
	}

}
