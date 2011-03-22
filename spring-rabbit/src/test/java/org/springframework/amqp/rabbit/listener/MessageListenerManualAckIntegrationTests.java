package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;

import com.rabbitmq.client.Channel;

public class MessageListenerManualAckIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerManualAckIntegrationTests.class);

	private Queue queue = new Queue("test.queue");

	private RabbitTemplate template = new RabbitTemplate();

	private int concurrentConsumers = 1;

	private int messageCount = 50;

	private int txSize = 1;
	
	private boolean transactional = false;
	
	private SimpleMessageListenerContainer container;

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.DEBUG, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class);

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueue(queue);

	@Before
	public void createConnectionFactory() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getPort());
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
	public void testListenerWithManualAckNonTransactional() throws Exception {
		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(new TestListener(latch));
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
	public void testListenerWithManualAckTransactional() throws Exception {
		transactional = true;
		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(new TestListener(latch));
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		int timeout = Math.min(1 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);
		assertNull(template.receiveAndConvert(queue.getName()));
	}

	private SimpleMessageListenerContainer createContainer(Object listener) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setQueueNames(queue.getName());
		container.setTxSize(txSize);
		container.setPrefetchCount(txSize);
		container.setConcurrentConsumers(concurrentConsumers);
		container.setChannelTransacted(transactional);
		container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	public static class TestListener implements ChannelAwareMessageListener {

		private final CountDownLatch latch;

		public TestListener(CountDownLatch latch) {
			this.latch = latch;
		}

		public void handleMessage(String value) {
		}

		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			try {
				logger.debug("Acking: " + value);
				channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
			} finally {
				latch.countDown();
			}
		}
	}

}
