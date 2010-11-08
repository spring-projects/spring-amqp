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
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;

@RunWith(Parameterized.class)
public class SimpleMessageListenerContainerIntegrationTests {

	private static Log logger = LogFactory.getLog(SimpleMessageListenerContainerIntegrationTests.class);

	private Queue queue;

	private RabbitTemplate template = new RabbitTemplate();

	private final int concurrentConsumers;

	private final boolean transactional;

	@Rule
	public LogLevelAdjuster logLevels = new LogLevelAdjuster(Level.ERROR, RabbitTemplate.class,
			SimpleMessageListenerContainer.class);

	@Rule
	public static BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	private final int messageCount;

	private SimpleMessageListenerContainer container;

	public SimpleMessageListenerContainerIntegrationTests(int messageCount, int concurrency, boolean transacted) {
		this.messageCount = messageCount;
		this.concurrentConsumers = concurrency;
		this.transactional = transacted;
	}

	@Parameters
	public static List<Object[]> getParameters() {
		return Arrays.asList(new Object[] { 1, 1, true }, new Object[] { 1, 1, false }, new Object[] { 4, 1, true },
				new Object[] { 2, 2, true }, new Object[] { 2, 2, true }, new Object[] { 20, 4, true }, new Object[] {
						20, 4, false }, new Object[] { 1000, 4, true }, new Object[] { 1000, 4, false });
	}

	@Before
	public void declareQueue() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		// connectionFactory.setPort(5673);
		template.setConnectionFactory(connectionFactory);
		RabbitAdmin admin = new RabbitAdmin(template);
		try {
			admin.deleteQueue("test.queue");
		} catch (AmqpIOException e) {
			// Ignore (queue didn't exist)
		}
		queue = new Queue("test.queue");
		// Idempotent, so no problem to do this for every test
		admin.declareQueue(queue);
		admin.purgeQueue("test.queue", false);
	}

	@After
	public void clear() throws Exception {
		// Wait for broker communication to finish before trying to stop container
		Thread.sleep(300L);
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
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
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
		if (transactional) {
			assertNotNull(template.receiveAndConvert(queue.getName()));
		} else {
			assertNull(template.receiveAndConvert(queue.getName()));
		}
	}

	private SimpleMessageListenerContainer createContainer(Object listener) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setQueueName(queue.getName());
		container.setConcurrentConsumers(concurrentConsumers);
		container.setChannelTransacted(transactional);
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

}
