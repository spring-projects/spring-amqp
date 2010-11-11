package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

@RunWith(Parameterized.class)
public class SimpleMessageListenerContainerIntegrationTests {

	private static Log logger = LogFactory.getLog(SimpleMessageListenerContainerIntegrationTests.class);

	private enum TransactionType {
		NONE, NATIVE, EXTERNAL;
		public boolean isTransactional() {
			return this != NONE;
		}
	}

	private Queue queue = new Queue("test.queue");

	private RabbitTemplate template = new RabbitTemplate();

	private final int concurrentConsumers;

	private final TransactionType transactional;

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.ERROR, RabbitTemplate.class,
			SimpleMessageListenerContainer.class);

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueue(queue);

	private final int messageCount;

	private SimpleMessageListenerContainer container;

	private final int txSize;

	public SimpleMessageListenerContainerIntegrationTests(int messageCount, int concurrency,
			TransactionType transacted, int txSize) {
		this.messageCount = messageCount;
		this.concurrentConsumers = concurrency;
		this.transactional = transacted;
		this.txSize = txSize;
	}

	@Parameters
	public static List<Object[]> getParameters() {
		return Arrays.asList(params(0, 1, 1, TransactionType.NATIVE), params(1, 1, 1, TransactionType.NONE),
				params(2, 4, 1, TransactionType.NATIVE), params(3, 4, 1, TransactionType.EXTERNAL),
				params(4, 2, 2, TransactionType.NATIVE), params(5, 2, 2, TransactionType.NONE),
				params(6, 20, 4, TransactionType.NATIVE), params(7, 20, 4, TransactionType.NONE),
				params(8, 1000, 4, TransactionType.NATIVE), params(9, 1000, 4, TransactionType.NONE),
				params(10, 1000, 4, TransactionType.NATIVE, 10));
	}

	private static Object[] params(int i, int messageCount, int concurrency, TransactionType transacted, int txSize) {
		return new Object[] { messageCount, concurrency, transacted, txSize };
	}

	private static Object[] params(int i, int messageCount, int concurrency, TransactionType transacted) {
		return params(i, messageCount, concurrency, transacted, 1);
	}

	@Before
	public void declareQueue() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		// connectionFactory.setPort(5673);
		template.setConnectionFactory(connectionFactory);
	}

	@After
	public void clear() throws Exception {
		// Wait for broker communication to finish before trying to stop container
		Thread.sleep(300L);
		logger.debug("Shutting down at end of test");
		if (container != null) {
			container.shutdown();
		}
	}

	@Test
	public void testListenerSunnyDay() throws Exception {
		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(new PojoListener(latch));
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		boolean waited = latch.await(Math.max(1, messageCount / 100), TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);
		assertNull(template.receiveAndConvert(queue.getName()));
	}

	@Test
	public void testListenerWithException() throws Exception {
		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(new PojoListener(latch, true));
		if (transactional.isTransactional()) {
			// Should only need one message if it is going to fail
			template.convertAndSend(queue.getName(), "foo");
		} else {
			for (int i = 0; i < messageCount; i++) {
				template.convertAndSend(queue.getName(), i + "foo");
			}
		}
		try {
			boolean waited = latch.await(Math.max(1, messageCount / 100), TimeUnit.SECONDS);
			assertTrue("Timed out waiting for message", waited);
		} finally {
			// Wait for broker communication to finish before trying to stop
			// container
			Thread.sleep(300L);
			container.shutdown();
			Thread.sleep(300L);
		}
		if (transactional.isTransactional()) {
			assertNotNull(template.receiveAndConvert(queue.getName()));
		} else {
			assertNull(template.receiveAndConvert(queue.getName()));
		}
	}

	private SimpleMessageListenerContainer createContainer(Object listener) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setQueueName(queue.getName());
		container.setTxSize(txSize);
		container.setPrefetchCount(txSize);
		container.setConcurrentConsumers(concurrentConsumers);
		container.setChannelTransacted(transactional.isTransactional());
		if (transactional == TransactionType.EXTERNAL) {
			container.setTransactionManager(new TestTransactionManager());
		}
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	public static class PojoListener {
		private AtomicInteger count = new AtomicInteger();

		private final CountDownLatch latch;

		private final boolean fail;

		public PojoListener(CountDownLatch latch) {
			this(latch, false);
		}

		public PojoListener(CountDownLatch latch, boolean fail) {
			this.latch = latch;
			this.fail = fail;
		}

		public void handleMessage(String value) {
			try {
				int counter = count.getAndIncrement();
				if (logger.isDebugEnabled() && counter % 500 == 0) {
					logger.debug(value + counter);
				}
				if (fail) {
					throw new RuntimeException("Planned failure");
				}
			} finally {
				latch.countDown();
			}
		}
	}

	@SuppressWarnings("serial")
	private class TestTransactionManager extends AbstractPlatformTransactionManager {

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		}

		@Override
		protected Object doGetTransaction() throws TransactionException {
			return new Object();
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		}

	}

}
