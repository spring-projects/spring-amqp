package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
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
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.MessageListenerBrokerInterruptionIntegrationTests.VanillaListener;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;

import com.rabbitmq.client.Channel;

public class MessageListenerRecoveryCachingConnectionIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerRecoveryCachingConnectionIntegrationTests.class);

	private Queue queue = new Queue("test.queue");

	private int concurrentConsumers = 1;

	private int messageCount = 10;

	private int txSize = 1;

	private boolean transactional = false;

	private AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	private SimpleMessageListenerContainer container;

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.DEBUG, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class);

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueue(queue);

	protected ConnectionFactory createConnectionFactory() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
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
	}

	@Test
	public void testListenerSendsMessageAndThenCommit() throws Exception {

		ConnectionFactory connectionFactory = createConnectionFactory();
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		Queue sendQueue = new Queue("test.send");
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

		// All messages committed
		assertEquals("bar", new String((byte[]) template.receiveAndConvert(sendQueue.getName())));
		assertNull(template.receiveAndConvert(queue.getName()));

	}

	@Test
	public void testListenerSendsMessageAndThenRollback() throws Exception {

		ConnectionFactory connectionFactory = createConnectionFactory();
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		Queue sendQueue = new Queue("test.send");
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

		// Foo message is redelivered
		assertEquals("foo", template.receiveAndConvert(queue.getName()));
		// Sending of bar message is also rolled back
		assertNull(template.receiveAndConvert(sendQueue.getName()));

	}

	@Test
	public void testListenerRecoversFromBogusDoubleAck() throws Exception {

		RabbitTemplate template = new RabbitTemplate(createConnectionFactory());

		acknowledgeMode = AcknowledgeMode.MANUAL;

		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(queue.getName(), new ManualAckListener(latch), createConnectionFactory());
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = getTimeout();
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

	}

	@Test
	public void testListenerRecoversFromClosedChannel() throws Exception {

		RabbitTemplate template = new RabbitTemplate(createConnectionFactory());

		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(queue.getName(), new AbortChannelListener(latch), createConnectionFactory());
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = getTimeout();
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

	}

	@Test
	public void testListenerRecoversFromClosedChannelAndStop() throws Exception {

		RabbitTemplate template = new RabbitTemplate(createConnectionFactory());

		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(queue.getName(), new AbortChannelListener(latch), createConnectionFactory());
		Thread.sleep(500L);
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

	}

	@Test
	public void testListenerRecoversFromClosedConnection() throws Exception {

		RabbitTemplate template = new RabbitTemplate(createConnectionFactory());

		CountDownLatch latch = new CountDownLatch(messageCount);
		ConnectionFactory connectionFactory = createConnectionFactory();
		container = createContainer(queue.getName(),
				new CloseConnectionListener((ConnectionProxy) connectionFactory.createConnection(), latch),
				connectionFactory);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = Math.min(4 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

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
		}

		int timeout = getTimeout();
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

	}

	@Test(expected = AmqpIllegalStateException.class)
	public void testListenerDoesNotRecoverFromMissingQueue() throws Exception {
		concurrentConsumers = 3;
		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer("nonexistent", new VanillaListener(latch), createConnectionFactory());
	}

	@Test(expected = AmqpIllegalStateException.class)
	public void testSingleListenerDoesNotRecoverFromMissingQueue() throws Exception {
		/*
		 * A single listener sometimes doesn't have time to attempt to start before we ask it if it has failed, so this
		 * is a good test of that potential bug.
		 */
		concurrentConsumers = 1;
		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer("nonexistent", new VanillaListener(latch), createConnectionFactory());
	}

	private int getTimeout() {
		return Math.min(1 + messageCount / (4 * concurrentConsumers), 30);
	}

	private SimpleMessageListenerContainer createContainer(String queueName, Object listener,
			ConnectionFactory connectionFactory) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setQueueNames(queueName);
		container.setTxSize(txSize);
		container.setPrefetchCount(txSize);
		container.setConcurrentConsumers(concurrentConsumers);
		container.setChannelTransacted(transactional);
		container.setAcknowledgeMode(acknowledgeMode);
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	public static class ManualAckListener implements ChannelAwareMessageListener {

		private AtomicBoolean failed = new AtomicBoolean(false);

		private final CountDownLatch latch;

		public ManualAckListener(CountDownLatch latch) {
			this.latch = latch;
		}

		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			try {
				logger.debug("Acking: " + value);
				channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
				if (failed.compareAndSet(false, true)) {
					// intentional error (causes exception on connection thread):
					channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
				}
			} finally {
				latch.countDown();
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
			} finally {
				latch.countDown();
			}
		}
	}

	public static class AbortChannelListener implements ChannelAwareMessageListener {

		private AtomicBoolean failed = new AtomicBoolean(false);

		private final CountDownLatch latch;

		public AbortChannelListener(CountDownLatch latch) {
			this.latch = latch;
		}

		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			logger.debug("Receiving: " + value);
			if (failed.compareAndSet(false, true)) {
				// intentional error (causes exception on connection thread):
				channel.abort();
			} else {
				latch.countDown();
			}
		}
	}

	public static class CloseConnectionListener implements ChannelAwareMessageListener {

		private AtomicBoolean failed = new AtomicBoolean(false);

		private final CountDownLatch latch;

		private final Connection connection;

		public CloseConnectionListener(ConnectionProxy connection, CountDownLatch latch) {
			this.connection = connection.getTargetConnection();
			this.latch = latch;
		}

		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			logger.debug("Receiving: " + value);
			if (failed.compareAndSet(false, true)) {
				// intentional error (causes exception on connection thread):
				connection.close();
			} else {
				latch.countDown();
			}
		}
	}
}
