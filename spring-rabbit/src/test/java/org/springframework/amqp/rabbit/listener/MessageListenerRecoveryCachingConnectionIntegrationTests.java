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
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionProxy;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
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
	public void testListenerRecoversFromBogusDoubleAck() throws Exception {

		RabbitTemplate template = new RabbitTemplate(createConnectionFactory());

		acknowledgeMode = AcknowledgeMode.MANUAL;

		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(new ManualAckListener(latch), createConnectionFactory());
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = Math.min(1 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

	}

	@Test
	public void testListenerRecoversFromClosedChannel() throws Exception {

		RabbitTemplate template = new RabbitTemplate(createConnectionFactory());

		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(new AbortChannelListener(latch), createConnectionFactory());
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = Math.min(1 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

	}

	@Test
	public void testListenerRecoversFromClosedChannelAndStop() throws Exception {

		RabbitTemplate template = new RabbitTemplate(createConnectionFactory());

		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(new AbortChannelListener(latch), createConnectionFactory());
		assertEquals(concurrentConsumers, container.getActiveConsumerCount());

		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = Math.min(1 + messageCount / (4 * concurrentConsumers), 30);
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
		container = createContainer(new CloseConnectionListener((ConnectionProxy) connectionFactory.createConnection(),
				latch), connectionFactory);
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
		container = createContainer(new ManualAckListener(latch), connectionFactory);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = Math.min(1 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		// TODO: is there a race condition here where the template gets a closed connection and this receive fails?
		assertNull(template.receiveAndConvert(queue.getName()));

	}

	private SimpleMessageListenerContainer createContainer(Object listener, ConnectionFactory connectionFactory) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setQueueName(queue.getName());
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
