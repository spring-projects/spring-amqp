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

@RunWith(Parameterized.class)
public class SimpleMessageListenerContainerIntegrationTests {

	private static Log logger = LogFactory.getLog(SimpleMessageListenerContainerIntegrationTests.class);

	private Queue queue;

	private RabbitTemplate template = new RabbitTemplate();

	private final int concurrentConsumers;

	private final boolean transactional;

	@Rule
	public static BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	private final int messageCount;

	public SimpleMessageListenerContainerIntegrationTests(int messageCount, int concurrency, boolean transacted) {
		this.messageCount = messageCount;
		this.concurrentConsumers = concurrency;
		this.transactional = transacted;
	}

	@Parameters
	public static List<Object[]> getParameters() {
		// return Collections.singletonList(new Object[] { 1, 1, true });
		return Arrays.asList(new Object[] { 1, 1, true }, new Object[] { 1, 1, false }, new Object[] { 4, 1, true }, new Object[] { 2, 2, true },
				new Object[] { 2, 2, true }, new Object[] { 20, 4, true }, new Object[] { 20, 4, false });
	}

	@Before
	public void declareQueue() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		template.setConnectionFactory(connectionFactory);
		RabbitAdmin admin = new RabbitAdmin(template);
		try {
			admin.deleteQueue("test.queue");
		}
		catch (AmqpIOException e) {
			// Ignore (queue didn't exist)
		}
		queue = new Queue("test.queue");
		// Idempotent, so no problem to do this for every test
		admin.declareQueue(queue);
		admin.purgeQueue("test.queue", false);
	}

	@Test
	public void testListenerSunnyDay() throws Exception {
		CountDownLatch latch = new CountDownLatch(messageCount);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i+"foo");
		}
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		container.setChannelTransacted(transactional);
		container.setConcurrentConsumers(concurrentConsumers);
		container.setQueueName(queue.getName());
		container.afterPropertiesSet();
		container.start();
		try {
			boolean waited = latch.await(1, TimeUnit.SECONDS);
			assertTrue("Timed out waiting for message", waited);
		}
		finally {
			// Wait for broker communication to finish before trying to stop
			// container
			Thread.sleep(300L);
			container.shutdown();
		}
		assertNull(template.receiveAndConvert(queue.getName()));
	}

	@Test
	public void testListenerWithException() throws Exception {
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i+"foo");
		}
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		CountDownLatch latch = new CountDownLatch(messageCount);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch, true)));
		container.setQueueName(queue.getName());
		container.setConcurrentConsumers(concurrentConsumers);
		container.setChannelTransacted(transactional);
		container.afterPropertiesSet();
		container.start();
		try {
			boolean waited = latch.await(2, TimeUnit.SECONDS);
			assertTrue("Timed out waiting for message", waited);
		}
		finally {
			// Wait for broker communication to finish before trying to stop
			// container
			Thread.sleep(300L);
			container.shutdown();
			Thread.sleep(300L);
		}
		if (transactional) {
			assertNotNull(template.receiveAndConvert(queue.getName()));
		}
		else {
			assertNull(template.receiveAndConvert(queue.getName()));
		}
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
				logger.debug(value + count.getAndIncrement());
				if (fail) {
					throw new RuntimeException("Planned failure");
				}
			}
			finally {
				latch.countDown();
			}
		}
	}

}
