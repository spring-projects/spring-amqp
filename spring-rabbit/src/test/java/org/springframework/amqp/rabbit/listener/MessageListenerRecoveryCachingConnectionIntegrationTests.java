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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionProxy;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LongRunningIntegrationTest;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.support.GenericApplicationContext;

import com.rabbitmq.client.Channel;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.0
 *
 */
public class MessageListenerRecoveryCachingConnectionIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerRecoveryCachingConnectionIntegrationTests.class);

	private final Queue queue = new Queue("test.queue");

	private final Queue sendQueue = new Queue("test.send");

	private int concurrentConsumers = 1;

	private final int messageCount = 10;

	private boolean transactional = false;

	private AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	private SimpleMessageListenerContainer container;

	@Rule
	public LongRunningIntegrationTest longTests = new LongRunningIntegrationTest();

	@Rule
	public LogLevelAdjuster logLevels = new LogLevelAdjuster(Level.DEBUG, RabbitTemplate.class, ManualAckListener.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class, CachingConnectionFactory.class);

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue.getName(), sendQueue.getName());

	protected CachingConnectionFactory createConnectionFactory() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		return connectionFactory;
	}

	@After
	public void clear() throws Exception {
		// Wait for broker communication to finish before trying to stop container
		Thread.sleep(300L);
		logger.debug("Shutting down at end of test");
		if (container != null) {
			container.shutdown();
		}
		this.brokerIsRunning.removeTestQueues();
	}

	@Test
	public void testListenerSendsMessageAndThenContainerCommits() throws Exception {

		ConnectionFactory connectionFactory = createConnectionFactory();
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		new RabbitAdmin(connectionFactory).declareQueue(sendQueue);

		acknowledgeMode = AcknowledgeMode.AUTO;
		transactional = true;

		CountDownLatch latch = new CountDownLatch(1);
		container = createContainer(queue.getName(), new ChannelSenderListener(sendQueue.getName(), latch, false),
				connectionFactory);
		template.convertAndSend(queue.getName(), "foo");

		int timeout = getTimeout();
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		// Give message time to reach broker (intermittent test failures)!
		Thread.sleep(500L);
		// All messages committed
		byte[] bytes = (byte[]) template.receiveAndConvert(sendQueue.getName());
		assertNotNull(bytes);
		assertEquals("bar", new String(bytes));
		assertEquals(null, template.receiveAndConvert(queue.getName()));

		this.container.stop();
		((DisposableBean) connectionFactory).destroy();

	}

	@Test
	public void testListenerSendsMessageAndThenRollback() throws Exception {

		ConnectionFactory connectionFactory = createConnectionFactory();
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		new RabbitAdmin(connectionFactory).declareQueue(sendQueue);

		acknowledgeMode = AcknowledgeMode.AUTO;
		transactional = true;

		CountDownLatch latch = new CountDownLatch(1);
		container = createContainer(queue.getName(), new ChannelSenderListener(sendQueue.getName(), latch, true),
				connectionFactory);
		template.convertAndSend(queue.getName(), "foo");

		int timeout = getTimeout();
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		container.stop();
		Thread.sleep(200L);

		// Foo message is redelivered
		assertEquals("foo", template.receiveAndConvert(queue.getName()));
		// Sending of bar message is also rolled back
		assertNull(template.receiveAndConvert(sendQueue.getName()));

		((DisposableBean) connectionFactory).destroy();

	}

	@Test
	public void testListenerRecoversFromBogusDoubleAck() throws Exception {

		ConnectionFactory connectionFactory1 = createConnectionFactory();
		RabbitTemplate template = new RabbitTemplate(connectionFactory1);

		acknowledgeMode = AcknowledgeMode.MANUAL;

		CountDownLatch latch = new CountDownLatch(messageCount);
		ConnectionFactory connectionFactory2 = createConnectionFactory();
		container = createContainer(queue.getName(), new ManualAckListener(latch), connectionFactory2);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = getTimeout();
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

		this.container.stop();
		((DisposableBean) connectionFactory1).destroy();
		((DisposableBean) connectionFactory2).destroy();
	}

	@Test
	public void testListenerRecoversFromClosedChannel() throws Exception {

		ConnectionFactory connectionFactory1 = createConnectionFactory();
		RabbitTemplate template = new RabbitTemplate(connectionFactory1);

		CountDownLatch latch = new CountDownLatch(messageCount);
		ConnectionFactory connectionFactory2 = createConnectionFactory();
		container = createContainer(queue.getName(), new AbortChannelListener(latch), connectionFactory2);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = getTimeout();
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

		this.container.stop();
		((DisposableBean) connectionFactory1).destroy();
		((DisposableBean) connectionFactory2).destroy();
	}

	@Test
	public void testListenerRecoversFromClosedChannelAndStop() throws Exception {

		ConnectionFactory connectionFactory1 = createConnectionFactory();
		RabbitTemplate template = new RabbitTemplate(connectionFactory1);

		CountDownLatch latch = new CountDownLatch(messageCount);
		ConnectionFactory connectionFactory2 = createConnectionFactory();
		container = createContainer(queue.getName(), new AbortChannelListener(latch), connectionFactory2);
		int n = 0;
		while (n++ < 100 && container.getActiveConsumerCount() != concurrentConsumers) {
			Thread.sleep(50L);
		}
		assertEquals(concurrentConsumers, container.getActiveConsumerCount());

		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = getTimeout();
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

		assertEquals(concurrentConsumers, container.getActiveConsumerCount());
		container.stop();
		assertEquals(0, container.getActiveConsumerCount());

		((DisposableBean) connectionFactory1).destroy();
		((DisposableBean) connectionFactory2).destroy();

	}

	@Test
	public void testListenerRecoversFromClosedConnection() throws Exception {

		ConnectionFactory connectionFactory1 = createConnectionFactory();
		RabbitTemplate template = new RabbitTemplate(connectionFactory1);

		CountDownLatch latch = new CountDownLatch(messageCount);
		CachingConnectionFactory connectionFactory2 = createConnectionFactory();
		// this test closes the underlying connection normally; it won't automatically recover.
		connectionFactory2.getRabbitConnectionFactory().setAutomaticRecoveryEnabled(false);
		container = createContainer(queue.getName(),
				new CloseConnectionListener((ConnectionProxy) connectionFactory2.createConnection(), latch),
				connectionFactory2);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = Math.min(4 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

		this.container.stop();
		((DisposableBean) connectionFactory1).destroy();
		((DisposableBean) connectionFactory2).destroy();

	}

	@Test
	public void testListenerRecoversAndTemplateSharesConnectionFactory() throws Exception {

		ConnectionFactory connectionFactory = createConnectionFactory();
		RabbitTemplate template = new RabbitTemplate(connectionFactory);

		acknowledgeMode = AcknowledgeMode.MANUAL;

		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(queue.getName(), new ManualAckListener(latch), connectionFactory);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
			// Give the listener container a chance to steal the connection from the template
			Thread.sleep(200);
		}

		int timeout = getTimeout();
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

		this.container.stop();
		((DisposableBean) connectionFactory).destroy();

	}

	@Test(expected = AmqpIllegalStateException.class)
	public void testListenerDoesNotRecoverFromMissingQueue() throws Exception {

		concurrentConsumers = 3;
		CountDownLatch latch = new CountDownLatch(messageCount);

		ConnectionFactory connectionFactory = createConnectionFactory();
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		admin.deleteQueue("nonexistent");

		try {
			container = createContainer("nonexistent", new VanillaListener(latch), connectionFactory);
		}
		finally {
			((DisposableBean) connectionFactory).destroy();
		}
	}

	@Test(expected = AmqpIllegalStateException.class)
	public void testSingleListenerDoesNotRecoverFromMissingQueue() throws Exception {
		/*
		 * A single listener sometimes doesn't have time to attempt to start before we ask it if it has failed, so this
		 * is a good test of that potential bug.
		 */
		concurrentConsumers = 1;
		CountDownLatch latch = new CountDownLatch(messageCount);

		ConnectionFactory connectionFactory = createConnectionFactory();
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		admin.deleteQueue("nonexistent");
		try {
			container = createContainer("nonexistent", new VanillaListener(latch), connectionFactory);
		}
		finally {
			((DisposableBean) connectionFactory).destroy();
		}
	}

	@Test
	public void testSingleListenerDoesRecoverFromMissingQueueWhenNotFatal() throws Exception {
		concurrentConsumers = 1;
		CountDownLatch latch = new CountDownLatch(messageCount);

		ConnectionFactory connectionFactory = createConnectionFactory();
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		admin.deleteQueue("nonexistent");
		try {
			container = doCreateContainer("nonexistent", new VanillaListener(latch), connectionFactory);
			container.setMissingQueuesFatal(false);
			container.start();
			testRecoverMissingQueues(latch, connectionFactory);
		}
		finally {
			if (container != null) {
				container.stop();
			}
			admin.deleteQueue("nonexistent");
			((DisposableBean) connectionFactory).destroy();
		}
	}

	@Test
	public void testSingleListenerDoesRecoverFromMissingQueueWhenNotFatalGlobalProps() throws Exception {
		concurrentConsumers = 1;
		CountDownLatch latch = new CountDownLatch(messageCount);

		ConnectionFactory connectionFactory = createConnectionFactory();
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		admin.deleteQueue("nonexistent");
		try {
			container = doCreateContainer("nonexistent", new VanillaListener(latch), connectionFactory);
			Properties properties = new Properties();
			properties.setProperty("mlc.missing.queues.fatal", "false");
			GenericApplicationContext context = new GenericApplicationContext();
			context.getBeanFactory().registerSingleton("spring.amqp.global.properties", properties);
			context.refresh();
			container.setApplicationContext(context);
			container.start();
			testRecoverMissingQueues(latch, connectionFactory);
		}
		finally {
			if (container != null) {
				container.stop();
			}
			admin.deleteQueue("nonexistent");
			((DisposableBean) connectionFactory).destroy();
		}
	}

	private void testRecoverMissingQueues(CountDownLatch latch, ConnectionFactory connectionFactory)
			throws InterruptedException {
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		// queue doesn't exist during startup - verify we started, create queue and verify recovery
		Thread.sleep(1000);
		assertEquals(messageCount, latch.getCount());
		admin.declareQueue(new Queue("nonexistent"));
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend("nonexistent", "foo" + i);
		}
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		Set<?> consumers = TestUtils.getPropertyValue(container, "consumers", Set.class);
		assertEquals(1, consumers.size());
		Object consumer = consumers.iterator().next();

		// delete the queue and verify we recover again when it is recreated.
		admin.deleteQueue("nonexistent");
		Thread.sleep(1000);
		latch = new CountDownLatch(messageCount);
		container.setMessageListener(new MessageListenerAdapter(new VanillaListener(latch)));
		assertEquals(messageCount, latch.getCount());
		admin.declareQueue(new Queue("nonexistent"));
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend("nonexistent", "foo" + i);
		}
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertEquals(1, consumers.size());
		assertNotSame(consumer, consumers.iterator().next());
	}

	private int getTimeout() {
		return Math.min(1 + messageCount / (concurrentConsumers), 30);
	}

	private SimpleMessageListenerContainer createContainer(String queueName, Object listener,
			ConnectionFactory connectionFactory) {
		SimpleMessageListenerContainer container = doCreateContainer(queueName, listener, connectionFactory);
		container.start();
		return container;
	}

	protected SimpleMessageListenerContainer doCreateContainer(String queueName, Object listener,
			ConnectionFactory connectionFactory) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setQueueNames(queueName);
		container.setPrefetchCount(1);
		container.setConcurrentConsumers(concurrentConsumers);
		container.setChannelTransacted(transactional);
		container.setAcknowledgeMode(acknowledgeMode);
		container.setRecoveryInterval(100);
		container.setFailedDeclarationRetryInterval(100);
		container.setReceiveTimeout(50);
		container.afterPropertiesSet();
		return container;
	}

	public static class ManualAckListener implements ChannelAwareMessageListener {

		private final AtomicBoolean failed = new AtomicBoolean(false);

		private final CountDownLatch latch;

		private final Set<String> received = Collections.synchronizedSet(new HashSet<String>());

		public ManualAckListener(CountDownLatch latch) {
			this.latch = latch;
		}

		@Override
		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			logger.debug("Acking: " + value);
			channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
			if (failed.compareAndSet(false, true)) {
				// intentional error (causes exception on connection thread):
				channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
			}

			if (this.received.add(value)) {
				latch.countDown();
			}
			else {
				logger.debug(value + " already received, redelivered="
						+ message.getMessageProperties().isRedelivered());
			}
		}
	}

	public static class ChannelSenderListener implements ChannelAwareMessageListener {

		private final CountDownLatch latch;

		private final boolean fail;

		private final String queueName;

		public ChannelSenderListener(String queueName, CountDownLatch latch, boolean fail) {
			this.queueName = queueName;
			this.latch = latch;
			this.fail = fail;
		}

		@Override
		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			try {
				logger.debug("Received: " + value + " Sending: bar");
				channel.basicPublish("", queueName, null, "bar".getBytes());
				if (fail) {
					logger.debug("Failing (planned)");
					// intentional error (causes exception on connection thread):
					throw new RuntimeException("Planned");
				}
			}
			finally {
				latch.countDown();
			}
		}
	}

	public static class AbortChannelListener implements ChannelAwareMessageListener {

		private final AtomicBoolean failed = new AtomicBoolean(false);

		private final CountDownLatch latch;

		public AbortChannelListener(CountDownLatch latch) {
			this.latch = latch;
		}

		@Override
		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			logger.debug("Receiving: " + value);
			if (failed.compareAndSet(false, true)) {
				// intentional error (causes exception on connection thread):
				channel.abort();
			}
			else {
				latch.countDown();
			}
		}
	}

	public static class CloseConnectionListener implements ChannelAwareMessageListener {

		private final AtomicBoolean failed = new AtomicBoolean(false);

		private final CountDownLatch latch;

		private final Connection connection;

		public CloseConnectionListener(ConnectionProxy connection, CountDownLatch latch) {
			this.connection = connection.getTargetConnection();
			this.latch = latch;
		}

		@Override
		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			logger.debug("Receiving: " + value);
			if (failed.compareAndSet(false, true)) {
				// intentional error (causes exception on connection thread):
				connection.close();
			}
			else {
				latch.countDown();
			}
		}
	}

	public static class VanillaListener implements ChannelAwareMessageListener {

		private final CountDownLatch latch;

		public VanillaListener(CountDownLatch latch) {
			this.latch = latch;
		}

		@Override
		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			logger.debug("Receiving: " + value);
			latch.countDown();
		}
	}

}
