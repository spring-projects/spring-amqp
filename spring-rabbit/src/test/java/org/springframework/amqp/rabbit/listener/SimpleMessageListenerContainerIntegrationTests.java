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
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LongRunningIntegrationTest;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import com.rabbitmq.client.Channel;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @since 1.0
 *
 */
@RunWith(Parameterized.class)
public class SimpleMessageListenerContainerIntegrationTests {

	private static Log logger = LogFactory.getLog(SimpleMessageListenerContainerIntegrationTests.class);

	private final Queue queue = new Queue("test.queue");

	private final RabbitTemplate template = new RabbitTemplate();

	private final int concurrentConsumers;

	private final AcknowledgeMode acknowledgeMode;

	@Rule
	public LongRunningIntegrationTest longTests = new LongRunningIntegrationTest();

	@Rule
	public LogLevelAdjuster logLevels = new LogLevelAdjuster(Level.OFF, RabbitTemplate.class,
			ConditionalRejectingErrorHandler.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class, CachingConnectionFactory.class);

	@Rule
	public LogLevelAdjuster testLogLevels = new LogLevelAdjuster(Level.DEBUG,
			SimpleMessageListenerContainerIntegrationTests.class);

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue.getName());

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private final int messageCount;

	private SimpleMessageListenerContainer container;

	private final int txSize;

	private final boolean externalTransaction;

	private final boolean transactional;

	public SimpleMessageListenerContainerIntegrationTests(int messageCount, int concurrency,
			AcknowledgeMode acknowledgeMode, boolean transactional, int txSize, boolean externalTransaction) {
		this.messageCount = messageCount;
		this.concurrentConsumers = concurrency;
		this.acknowledgeMode = acknowledgeMode;
		this.transactional = transactional;
		this.txSize = txSize;
		this.externalTransaction = externalTransaction;
	}

	@Parameters
	public static List<Object[]> getParameters() {
		return Arrays.asList(
				params(0, 1, 1, AcknowledgeMode.AUTO),
				params(1, 1, 1, AcknowledgeMode.NONE),
				params(2, 4, 1, AcknowledgeMode.AUTO),
				extern(3, 4, 1, AcknowledgeMode.AUTO),
				params(4, 4, 1, AcknowledgeMode.AUTO, false),
				params(5, 2, 2, AcknowledgeMode.AUTO),
				params(6, 2, 2, AcknowledgeMode.NONE),
				params(7, 20, 4, AcknowledgeMode.AUTO),
				params(8, 20, 4, AcknowledgeMode.NONE),
				params(9, 300, 4, AcknowledgeMode.AUTO),
				params(10, 300, 4, AcknowledgeMode.NONE),
				params(11, 300, 4, AcknowledgeMode.AUTO, 10)
				);
	}

	private static Object[] params(int i, int messageCount, int concurrency, AcknowledgeMode acknowledgeMode,
			boolean transactional, int txSize) {
		// "i" is just a counter to make it easier to identify the test in the log
		return new Object[] { messageCount, concurrency, acknowledgeMode, transactional, txSize, false };
	}

	private static Object[] params(int i, int messageCount, int concurrency, AcknowledgeMode acknowledgeMode, int txSize) {
		// For this test always us a transaction if it makes sense...
		return params(i, messageCount, concurrency, acknowledgeMode, acknowledgeMode.isTransactionAllowed(), txSize);
	}

	private static Object[] params(int i, int messageCount, int concurrency, AcknowledgeMode acknowledgeMode,
			boolean transactional) {
		return params(i, messageCount, concurrency, acknowledgeMode, transactional, 1);
	}

	private static Object[] params(int i, int messageCount, int concurrency, AcknowledgeMode acknowledgeMode) {
		return params(i, messageCount, concurrency, acknowledgeMode, 1);
	}

	private static Object[] extern(int i, int messageCount, int concurrency, AcknowledgeMode acknowledgeMode) {
		return new Object[] { messageCount, concurrency, acknowledgeMode, true, 1, true };
	}

	@Before
	public void declareQueue() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
	}

	@After
	public void clear() throws Exception {
		// Wait for broker communication to finish before trying to stop container
		logger.debug("Shutting down at end of test");
		if (container != null) {
			container.shutdown();
		}
		((DisposableBean) template.getConnectionFactory()).destroy();
		this.brokerIsRunning.removeTestQueues();
	}

	@Test
	public void testPojoListenerSunnyDay() throws Exception {
		CountDownLatch latch = new CountDownLatch(messageCount);
		doSunnyDayTest(latch, new MessageListenerAdapter(new PojoListener(latch)));
	}

	@Test
	public void testListenerSunnyDay() throws Exception {
		CountDownLatch latch = new CountDownLatch(messageCount);
		doSunnyDayTest(latch, new Listener(latch));
	}

	@Test
	public void testChannelAwareListenerSunnyDay() throws Exception {
		CountDownLatch latch = new CountDownLatch(messageCount);
		doSunnyDayTest(latch, new ChannelAwareListener(latch));
	}

	@Test
	public void testPojoListenerWithException() throws Exception {
		CountDownLatch latch = new CountDownLatch(messageCount);
		doListenerWithExceptionTest(latch, new MessageListenerAdapter(new PojoListener(latch, true)));
	}

	@Test
	public void testListenerWithException() throws Exception {
		CountDownLatch latch = new CountDownLatch(messageCount);
		doListenerWithExceptionTest(latch, new Listener(latch, true));
	}

	@Test
	public void testChannelAwareListenerWithException() throws Exception {
		CountDownLatch latch = new CountDownLatch(messageCount);
		doListenerWithExceptionTest(latch, new ChannelAwareListener(latch, true));
	}

	@Test
	public void testNullQueue() throws Exception {
		exception.expect(IllegalArgumentException.class);
		container = createContainer((MessageListener) (m) -> { }, (Queue) null);
	}

	@Test
	public void testNullQueueName() throws Exception {
		exception.expect(IllegalArgumentException.class);
		container = createContainer((MessageListener) (m) -> { }, (String) null);
	}

	private void doSunnyDayTest(CountDownLatch latch, Object listener) throws Exception {
		container = createContainer(listener);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		boolean waited = latch.await(Math.max(10, messageCount / 20), TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);
		assertNull(template.receiveAndConvert(queue.getName()));
	}

	private void doListenerWithExceptionTest(CountDownLatch latch, Object listener) throws Exception {
		container = createContainer(listener);
		if (acknowledgeMode.isTransactionAllowed()) {
			// Should only need one message if it is going to fail
			for (int i = 0; i < concurrentConsumers; i++) {
				template.convertAndSend(queue.getName(), i + "foo");
			}
		}
		else {
			for (int i = 0; i < messageCount; i++) {
				template.convertAndSend(queue.getName(), i + "foo");
			}
		}
		try {
			boolean waited = latch.await(10 + Math.max(1, messageCount / 10), TimeUnit.SECONDS);
			assertTrue("Timed out waiting for message", waited);
		}
		finally {
			container.shutdown();
		}
		if (acknowledgeMode.isTransactionAllowed()) {
			assertNotNull(template.receiveAndConvert(queue.getName()));
		}
		else {
			assertNull(template.receiveAndConvert(queue.getName()));
		}
	}

	private SimpleMessageListenerContainer createContainer(Object listener) {
		SimpleMessageListenerContainer container = createContainer(listener, queue.getName());
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	private SimpleMessageListenerContainer createContainer(Object listener, String queue) {
		SimpleMessageListenerContainer container = doCreateContainer(listener);
		container.setQueueNames(queue);
		return container;
	}

	private SimpleMessageListenerContainer createContainer(Object listener, Queue queue) {
		SimpleMessageListenerContainer container = doCreateContainer(listener);
		container.setQueues(queue);
		return container;
	}

	private SimpleMessageListenerContainer doCreateContainer(Object listener) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setMessageListener(listener);
		container.setTxSize(txSize);
		container.setPrefetchCount(txSize);
		container.setConcurrentConsumers(concurrentConsumers);
		container.setChannelTransacted(transactional);
		container.setAcknowledgeMode(acknowledgeMode);
		container.setBeanName("integrationTestContainer");
		container.setReceiveTimeout(50);
		// requires RabbitMQ 3.2.x
//		container.setConsumerArguments(Collections. <String, Object> singletonMap("x-priority", Integer.valueOf(10)));
		if (externalTransaction) {
			container.setTransactionManager(new TestTransactionManager());
		}
		return container;
	}

	public static class PojoListener {
		private final AtomicInteger count = new AtomicInteger();

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
				if (logger.isDebugEnabled() && counter % 100 == 0) {
					logger.debug("Handling: " + value + ":" + counter + " - " + latch);
				}
				if (fail) {
					throw new RuntimeException("Planned failure");
				}
			}
			finally {
				latch.countDown();
			}
		}
	}

	public static class Listener implements MessageListener {
		private final AtomicInteger count = new AtomicInteger();

		private final CountDownLatch latch;

		private final boolean fail;

		public Listener(CountDownLatch latch) {
			this(latch, false);
		}

		public Listener(CountDownLatch latch, boolean fail) {
			this.latch = latch;
			this.fail = fail;
		}

		@Override
		public void onMessage(Message message) {
			String value = new String(message.getBody());
			try {
				int counter = count.getAndIncrement();
				if (logger.isDebugEnabled() && counter % 100 == 0) {
					logger.debug(value + counter);
				}
				if (fail) {
					throw new RuntimeException("Planned failure");
				}
			}
			finally {
				latch.countDown();
			}
		}
	}

	public static class ChannelAwareListener implements ChannelAwareMessageListener {
		private final AtomicInteger count = new AtomicInteger();

		private final CountDownLatch latch;

		private final boolean fail;

		public ChannelAwareListener(CountDownLatch latch) {
			this(latch, false);
		}

		public ChannelAwareListener(CountDownLatch latch, boolean fail) {
			this.latch = latch;
			this.fail = fail;
		}

		@Override
		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			try {
				int counter = count.getAndIncrement();
				if (logger.isDebugEnabled() && counter % 100 == 0) {
					logger.debug(value + counter);
				}
				if (fail) {
					throw new RuntimeException("Planned failure");
				}
			}
			finally {
				latch.countDown();
			}
		}

	}

	@SuppressWarnings("serial")
	private class TestTransactionManager extends AbstractPlatformTransactionManager {

		TestTransactionManager() {
			super();
		}

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
