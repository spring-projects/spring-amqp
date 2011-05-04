package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;
import org.springframework.amqp.rabbit.test.RepeatProcessor;
import org.springframework.test.annotation.Repeat;

import com.rabbitmq.client.Channel;

/**
 * Long-running test created to facilitate profiling of SimpleMessageListenerContainer.
 * 
 * @author Dave Syer
 * 
 */
@Ignore
public class MessageListenerRecoveryRepeatIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerRecoveryRepeatIntegrationTests.class);

	private Queue queue = new Queue("test.queue");

	private Queue sendQueue = new Queue("test.send");

	private int concurrentConsumers = 1;

	private int messageCount = 2;

	private int txSize = 1;

	private boolean transactional = false;

	private AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	private SimpleMessageListenerContainer container;

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.INFO, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class);

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue, sendQueue);

	@Rule
	public RepeatProcessor repeatProcessor = new RepeatProcessor();

	private CloseConnectionListener listener;

	private ConnectionFactory connectionFactory;

	@Before
	public void init() {
		if (!repeatProcessor.isInitialized()) {
			logger.info("Initializing at start of test");
			connectionFactory = createConnectionFactory();
			listener = new CloseConnectionListener();
			container = createContainer(queue.getName(), listener, connectionFactory);
		}
	}

	@After
	public void clear() throws Exception {
		if (repeatProcessor.isFinalizing()) {
			// Wait for broker communication to finish before trying to stop container
			Thread.sleep(300L);
			logger.info("Shutting down at end of test");
			if (container != null) {
				container.shutdown();
			}
		}
	}

	@Test
	@Repeat(1000)
	public void testListenerRecoversFromClosedConnection() throws Exception {

		// logger.info("Testing...");

		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		CountDownLatch latch = new CountDownLatch(messageCount);
		listener.setLatch(latch);

		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = Math.min(4 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

	}

	private ConnectionFactory createConnectionFactory() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		// connectionFactory.setPort(BrokerTestUtils.getTracerPort());
		connectionFactory.setPort(BrokerTestUtils.getPort());
		return connectionFactory;
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
		container.setTaskExecutor(Executors.newFixedThreadPool(concurrentConsumers));
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	private static class CloseConnectionListener implements ChannelAwareMessageListener {

		private AtomicBoolean failed = new AtomicBoolean(false);

		private CountDownLatch latch;

		public void setLatch(CountDownLatch latch) {
			this.latch = latch;
			failed.set(false);
		}

		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			logger.info("Receiving: " + value);
			if (failed.compareAndSet(false, true)) {
				// intentional error (causes exception on connection thread):
				// channel.abort();
				// throw new RuntimeException("Planned");
				throw new FatalListenerExecutionException("Planned");
			} else {
				latch.countDown();
			}
		}
	}
}
