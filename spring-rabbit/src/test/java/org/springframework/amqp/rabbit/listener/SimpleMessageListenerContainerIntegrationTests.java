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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.BatchMessageListener;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.JUnitUtils;
import org.springframework.amqp.rabbit.junit.JUnitUtils.LevelsContainer;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @since 1.0
 *
 */
@RabbitAvailable(queues = SimpleMessageListenerContainerIntegrationTests.TEST_QUEUE)
@LogLevels(level = "OFF", classes = { RabbitTemplate.class,
		ConditionalRejectingErrorHandler.class,
		SimpleMessageListenerContainer.class, BlockingQueueConsumer.class, CachingConnectionFactory.class })
public class SimpleMessageListenerContainerIntegrationTests {

	public static final String TEST_QUEUE = "test.queue.SimpleMessageListenerContainerIntegrationTests";

	private static Log logger = LogFactory.getLog(SimpleMessageListenerContainerIntegrationTests.class);

	private static LevelsContainer levelsContainer;

	private final Queue queue = new Queue(TEST_QUEUE);

	private final RabbitTemplate template = new RabbitTemplate();

	private int concurrentConsumers;

	private AcknowledgeMode acknowledgeMode;

//	@Rule
//	public LogLevelAdjuster testLogLevels = new LogLevelAdjuster(Level.DEBUG,
//			SimpleMessageListenerContainerIntegrationTests.class);

	private int messageCount;

	private SimpleMessageListenerContainer container;

	private int txSize;

	private boolean externalTransaction;

	private boolean transactional;

	public static List<Object[]> getParameters() {
		return Arrays.asList(
				params(1, 1, AcknowledgeMode.AUTO),
				params(1, 1, AcknowledgeMode.NONE),
				params(4, 1, AcknowledgeMode.AUTO),
				extern(4, 1, AcknowledgeMode.AUTO),
				params(4, 1, AcknowledgeMode.AUTO, false),
				params(2, 2, AcknowledgeMode.AUTO),
				params(2, 2, AcknowledgeMode.NONE),
				params(20, 4, AcknowledgeMode.AUTO),
				params(20, 4, AcknowledgeMode.NONE),
				params(300, 4, AcknowledgeMode.AUTO),
				params(300, 4, AcknowledgeMode.NONE),
				params(300, 4, AcknowledgeMode.AUTO, 10)
				);
	}

	private static Object[] params(int messageCount, int concurrency, AcknowledgeMode acknowledgeMode,
			boolean transactional, int txSize) {

		return new Object[] { messageCount, concurrency, acknowledgeMode, transactional, txSize, false };
	}

	private static Object[] params(int messageCount, int concurrency, AcknowledgeMode acknowledgeMode, int txSize) {

		return params(messageCount, concurrency, acknowledgeMode, acknowledgeMode.isTransactionAllowed(), txSize);
	}

	private static Object[] params(int messageCount, int concurrency, AcknowledgeMode acknowledgeMode,
			boolean transactional) {

		return params(messageCount, concurrency, acknowledgeMode, transactional, 1);
	}

	private static Object[] params(int messageCount, int concurrency, AcknowledgeMode acknowledgeMode) {
		return params(messageCount, concurrency, acknowledgeMode, 1);
	}

	private static Object[] extern(int messageCount, int concurrency, AcknowledgeMode acknowledgeMode) {
		return new Object[] { messageCount, concurrency, acknowledgeMode, true, 1, true };
	}

	@BeforeAll
	public static void debugLog() {
		levelsContainer = JUnitUtils.adjustLogLevels("SimpleMessageListenerContainerIntegrationTests",
				Collections.singletonList(SimpleMessageListenerContainerIntegrationTests.class),
				Collections.emptyList(), Level.DEBUG);
	}

	@AfterAll
	public static void unDebugLog() {
		if (levelsContainer != null) {
			JUnitUtils.revertLevels("SimpleMessageListenerContainerIntegrationTests", levelsContainer);
		}
	}

	@BeforeEach
	public void declareQueue() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(4);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
	}

	@AfterEach
	public void clear() throws Exception {
		// Wait for broker communication to finish before trying to stop container
		logger.debug("Shutting down at end of test");
		if (container != null) {
			container.shutdown();
		}
		((DisposableBean) template.getConnectionFactory()).destroy();
	}

	@ParameterizedTest
	@MethodSource("getParameters")
	public void testPojoListenerSunnyDay(int count, int concurrency, AcknowledgeMode ackMode, boolean tx, int txSz,
			boolean externalTx) throws Exception {

		loadParams(count, concurrency, ackMode, tx, txSz, externalTx);
		CountDownLatch latch = new CountDownLatch(messageCount);
		doSunnyDayTest(latch, new MessageListenerAdapter(new PojoListener(latch)));
	}

	@ParameterizedTest
	@MethodSource("getParameters")
	public void testListenerSunnyDay(int count, int concurrency, AcknowledgeMode ackMode, boolean tx, int txSz,
			boolean externalTx) throws Exception {

		loadParams(count, concurrency, ackMode, tx, txSz, externalTx);
		CountDownLatch latch = new CountDownLatch(messageCount);
		doSunnyDayTest(latch, new Listener(latch));
	}

	@ParameterizedTest
	@MethodSource("getParameters")
	public void testChannelAwareListenerSunnyDay(int count, int concurrency, AcknowledgeMode ackMode, boolean tx, int txSz,
			boolean externalTx) throws Exception {

		loadParams(count, concurrency, ackMode, tx, txSz, externalTx);
		CountDownLatch latch = new CountDownLatch(messageCount);
		doSunnyDayTest(latch, new ChannelAwareListener(latch));
	}

	@ParameterizedTest
	@MethodSource("getParameters")
	public void testPojoListenerWithException(int count, int concurrency, AcknowledgeMode ackMode, boolean tx, int txSz,
			boolean externalTx) throws Exception {

		loadParams(count, concurrency, ackMode, tx, txSz, externalTx);
		CountDownLatch latch = new CountDownLatch(messageCount);
		doListenerWithExceptionTest(latch, new MessageListenerAdapter(new PojoListener(latch, true)));
	}

	@ParameterizedTest
	@MethodSource("getParameters")
	public void testListenerWithException(int count, int concurrency, AcknowledgeMode ackMode, boolean tx, int txSz,
			boolean externalTx) throws Exception {

		loadParams(count, concurrency, ackMode, tx, txSz, externalTx);
		CountDownLatch latch = new CountDownLatch(messageCount);
		doListenerWithExceptionTest(latch, new Listener(latch, true));
	}

	@ParameterizedTest
	@MethodSource("getParameters")
	public void testChannelAwareListenerWithException(int count, int concurrency, AcknowledgeMode ackMode, boolean tx, int txSz,
			boolean externalTx) throws Exception {

		loadParams(count, concurrency, ackMode, tx, txSz, externalTx);
		CountDownLatch latch = new CountDownLatch(messageCount);
		doListenerWithExceptionTest(latch, new ChannelAwareListener(latch, true));
	}

	@ParameterizedTest
	@MethodSource("getParameters")
	public void testNullQueue(int count, int concurrency, AcknowledgeMode ackMode, boolean tx, int txSz,
			boolean externalTx) {

		loadParams(count, concurrency, ackMode, tx, txSz, externalTx);
		assertThatIllegalArgumentException()
			.isThrownBy(() -> container = createContainer(m -> { }, (Queue) null));
	}

	@ParameterizedTest
	@MethodSource("getParameters")
	public void testNullQueueName(int count, int concurrency, AcknowledgeMode ackMode, boolean tx, int txSz,
			boolean externalTx) {

		loadParams(count, concurrency, ackMode, tx, txSz, externalTx);
		assertThatIllegalArgumentException()
			.isThrownBy(() -> container = createContainer(m -> { }, (String) null));
	}

	@ParameterizedTest
	@MethodSource("getParameters")
	public void testConsumerBatching(int count, int concurrency, AcknowledgeMode ackMode, boolean tx, int txSz,
			boolean externalTx) throws InterruptedException {

		loadParams(count, concurrency, ackMode, tx, txSz, externalTx);
		AtomicReference<List<Message>> received = new AtomicReference<>(new ArrayList<>());
		CountDownLatch latch = new CountDownLatch(1);
		this.container = createContainer((BatchMessageListener) messages -> {
			received.get().addAll(messages);
			if (received.get().size() == this.messageCount) {
				latch.countDown();
			}
		}, this.queue);
		this.container.setConsumerBatchEnabled(true);
		this.container.setBatchSize(this.messageCount);
		this.container.setConcurrentConsumers(1);
		this.container.afterPropertiesSet();
		this.container.start();
		for (int i = 0; i < this.messageCount; i++) {
			this.template.convertAndSend(this.queue.getName(), i + "foo");
		}
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(received.get()).isNotNull();
		assertThat(received.get()).hasSize(this.messageCount);
	}

	private void doSunnyDayTest(CountDownLatch latch, MessageListener listener) throws Exception {
		container = createContainer(listener);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		boolean waited = latch.await(Math.max(10, messageCount / 20), TimeUnit.SECONDS);
		assertThat(waited).as("Timed out waiting for message").isTrue();
		assertThat(template.receiveAndConvert(queue.getName())).isNull();
	}

	private void doListenerWithExceptionTest(CountDownLatch latch, MessageListener listener) throws Exception {
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
			assertThat(waited).as("Timed out waiting for message").isTrue();
		}
		finally {
			container.shutdown();
		}
		if (acknowledgeMode.isTransactionAllowed()) {
			await().untilAsserted(() -> assertThat(template.receiveAndConvert(queue.getName())).isNotNull());
		}
		else {
			assertThat(template.receiveAndConvert(queue.getName())).isNull();
		}
	}

	private SimpleMessageListenerContainer createContainer(MessageListener listener) {
		SimpleMessageListenerContainer container = createContainer(listener, queue.getName());
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	private SimpleMessageListenerContainer createContainer(MessageListener listener, String queue) {
		SimpleMessageListenerContainer container = doCreateContainer(listener);
		container.setQueueNames(queue);
		return container;
	}

	private SimpleMessageListenerContainer createContainer(MessageListener listener, Queue queue) {
		SimpleMessageListenerContainer container = doCreateContainer(listener);
		container.setQueues(queue);
		return container;
	}

	private SimpleMessageListenerContainer doCreateContainer(MessageListener listener) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setMessageListener(listener);
		container.setBatchSize(txSize);
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

	private void loadParams(int count, int concurrency, AcknowledgeMode ackMode, boolean tx, int txSz,
			boolean externalTx) {

		this.messageCount = count;
		this.concurrentConsumers = concurrency;
		this.acknowledgeMode = ackMode;
		this.transactional = tx;
		this.txSize = txSz;
		this.externalTransaction = externalTx;
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
