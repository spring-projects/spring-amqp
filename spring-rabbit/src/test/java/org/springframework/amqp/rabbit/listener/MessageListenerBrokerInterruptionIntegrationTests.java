package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.admin.QueueInfo;
import org.springframework.amqp.rabbit.admin.RabbitBrokerAdmin;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerPanic;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;

import com.rabbitmq.client.Channel;

public class MessageListenerBrokerInterruptionIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerBrokerInterruptionIntegrationTests.class);

	private Queue queue = new Queue("test.queue");

	private int concurrentConsumers = 1;

	private int messageCount = 10;

	private int txSize = 1;

	private boolean transactional = false;

	private AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	private SimpleMessageListenerContainer container;

	/*
	 * Ensure broker dies if a test fails (otherwise the erl process might have to be killed manually)
	 */
	@Rule
	public static BrokerPanic panic = new BrokerPanic();

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueue(queue);

	private ConnectionFactory connectionFactory;

	private RabbitBrokerAdmin brokerAdmin;

	public MessageListenerBrokerInterruptionIntegrationTests() throws Exception {
		FileUtils.deleteDirectory(new File("target/rabbitmq"));
		// Ensure queue is durable, or it won't survive the broker restart
		queue.setDurable(true);
		brokerIsRunning.setPort(BrokerTestUtils.getAdminPort());
		logger.debug("Setting up broker");
		brokerAdmin = BrokerTestUtils.getRabbitBrokerAdmin();
		panic.setBrokerAdmin(brokerAdmin);
		brokerAdmin.startNode();
	}

	@Before
	public void createConnectionFactory() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getAdminPort());
		this.connectionFactory = connectionFactory;
	}

	@After
	public void clear() throws Exception {
		// Wait for broker communication to finish before trying to stop container
		Thread.sleep(300L);
		logger.debug("Shutting down at end of test");
		if (container != null) {
			container.shutdown();
		}
		brokerAdmin.stopNode();
		// Remove all trace of the durable queue...
		FileUtils.deleteDirectory(new File("target/rabbitmq"));
	}

	@Test
	public void testListenerRecoversFromClosedConnection() throws Exception {

		List<QueueInfo> queues = brokerAdmin.getQueues();
		logger.info("Queues: " + queues);
		assertEquals(1, queues.size());
		assertTrue(queues.get(0).isDurable());

		RabbitTemplate template = new RabbitTemplate(connectionFactory);

		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(new VanillaListener(latch), connectionFactory);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		brokerAdmin.stopBrokerApplication();
		boolean waited = latch.await(500, TimeUnit.MILLISECONDS);
		assertFalse("Did not time out waiting for message", waited);

		container.stop();
		assertEquals(0, container.getActiveConsumerCount());

		brokerAdmin.startBrokerApplication();
		queues = brokerAdmin.getQueues();
		logger.info("Queues: " + queues);
		container.start();
		assertEquals(concurrentConsumers, container.getActiveConsumerCount());

		int timeout = Math.min(4 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

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

	public static class VanillaListener implements ChannelAwareMessageListener {

		private final CountDownLatch latch;

		public VanillaListener(CountDownLatch latch) {
			this.latch = latch;
		}

		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			logger.debug("Receiving: " + value);
			latch.countDown();
		}
	}
}
