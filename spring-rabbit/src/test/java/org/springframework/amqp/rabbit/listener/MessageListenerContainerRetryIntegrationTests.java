package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.AbstractRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.config.StatefulRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.config.StatelessRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.retry.policy.MapRetryContextCache;
import org.springframework.retry.support.RetryTemplate;

public class MessageListenerContainerRetryIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerContainerRetryIntegrationTests.class);

	private static Queue queue = new Queue("test.queue");

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue);

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.INFO, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class);

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private RabbitTemplate template;

	private RetryTemplate retryTemplate;

	private MessageConverter messageConverter;

	private RabbitTemplate createTemplate(int concurrentConsumers) {
		RabbitTemplate template = new RabbitTemplate();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
		if (messageConverter == null) {
			SimpleMessageConverter messageConverter = new SimpleMessageConverter();
			messageConverter.setCreateMessageIds(true);
			this.messageConverter = messageConverter;
		}
		template.setMessageConverter(messageConverter);
		return template;
	}

	@Test
	public void testStatefulRetryWithAllMessagesFailing() throws Exception {

		int messageCount = 10;
		int txSize = 1;
		int failFrequency = 1;
		int concurrentConsumers = 3;
		doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	@Test
	public void testStatelessRetryWithAllMessagesFailing() throws Exception {

		int messageCount = 10;
		int txSize = 1;
		int failFrequency = 1;
		int concurrentConsumers = 3;
		doTestStatelessRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	@Test
	public void testStatefulRetryWithNoMessageIds() throws Exception {

		int messageCount = 2;
		int txSize = 1;
		int failFrequency = 1;
		int concurrentConsumers = 1;
		SimpleMessageConverter messageConverter = new SimpleMessageConverter();
		// There will be no key for these messages so they cannot be recovered...
		messageConverter.setCreateMessageIds(false);
		this.messageConverter = messageConverter;
		// Beware of context cache busting if retry policy fails...
		this.retryTemplate = new RetryTemplate();
		this.retryTemplate.setRetryContextCache(new MapRetryContextCache(1));
		// The container should have shutdown, so there are now no active consumers
		exception.expectMessage("expected:<1> but was:<0>");
		doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	@Test
	public void testStatefulRetryWithTxSizeAndIntermittentFailure() throws Exception {

		int messageCount = 10;
		int txSize = 4;
		int failFrequency = 3;
		int concurrentConsumers = 3;
		doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	@Test
	public void testStatefulRetryWithMoreMessages() throws Exception {

		int messageCount = 200;
		int txSize = 10;
		int failFrequency = 6;
		int concurrentConsumers = 3;
		doTestStatefulRetry(messageCount, txSize, failFrequency, concurrentConsumers);

	}

	private Advice createRetryInterceptor(final CountDownLatch latch, boolean stateful) throws Exception {
		AbstractRetryOperationsInterceptorFactoryBean factory;
		if (stateful) {
			factory = new StatefulRetryOperationsInterceptorFactoryBean();
		} else {
			factory = new StatelessRetryOperationsInterceptorFactoryBean();
		}
		factory.setMessageRecoverer(new MessageRecoverer() {
			public void recover(Message message, Throwable cause) {
				logger.info("Recovered: " + message);
				latch.countDown();
			}
		});
		if (retryTemplate == null) {
			retryTemplate = new RetryTemplate();
		}
		factory.setRetryOperations(retryTemplate);
		Advice retryInterceptor = factory.getObject();
		return retryInterceptor;
	}

	private void doTestStatefulRetry(int messageCount, int txSize, int failFrequency, int concurrentConsumers)
			throws Exception {
		doTestRetry(messageCount, txSize, failFrequency, concurrentConsumers, true);
	}

	private void doTestStatelessRetry(int messageCount, int txSize, int failFrequency, int concurrentConsumers)
			throws Exception {
		doTestRetry(messageCount, txSize, failFrequency, concurrentConsumers, false);
	}

	private void doTestRetry(int messageCount, int txSize, int failFrequency, int concurrentConsumers, boolean stateful)
			throws Exception {

		int failedMessageCount = messageCount / failFrequency + (messageCount % failFrequency == 0 ? 0 : 1);

		template = createTemplate(concurrentConsumers);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), new Integer(i));
		}

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(
				template.getConnectionFactory());
		PojoListener listener = new PojoListener(failFrequency);
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.setChannelTransacted(true);
		container.setTxSize(txSize);
		container.setConcurrentConsumers(concurrentConsumers);

		final CountDownLatch latch = new CountDownLatch(failedMessageCount);
		container.setAdviceChain(new Advice[] { createRetryInterceptor(latch, stateful) });

		container.setQueueNames(queue.getName());
		container.afterPropertiesSet();
		container.start();

		try {

			int timeout = Math.min(1 + messageCount / concurrentConsumers, 30);

			final int count = messageCount;
			logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
			Executors.newSingleThreadExecutor().execute(new Runnable() {
				public void run() {
					while (container.getActiveConsumerCount() > 0) {
						try {
							Thread.sleep(100L);
						} catch (InterruptedException e) {
							latch.countDown();
							Thread.currentThread().interrupt();
							return;
						}
					}
					for (int i = 0; i < count; i++) {
						latch.countDown();
					}
				}
			});
			boolean waited = latch.await(timeout, TimeUnit.SECONDS);
			logger.info("All messages recovered: " + waited);
			assertEquals(concurrentConsumers, container.getActiveConsumerCount());
			assertTrue("Timed out waiting for messages", waited);

			// Retried each failure 3 times (default retry policy)...
			assertEquals(messageCount + 2 * failedMessageCount, listener.getCount());

		} finally {
			container.shutdown();
			assertEquals(0, container.getActiveConsumerCount());
		}

		// All failed messages recovered
		assertNull(template.receiveAndConvert(queue.getName()));

	}

	public static class PojoListener {
		private AtomicInteger count = new AtomicInteger();
		private final int failFrequency;

		public PojoListener(int failFrequency) {
			this.failFrequency = failFrequency;
		}

		public void handleMessage(int value) throws Exception {
			logger.debug(value+ ":" + count.getAndIncrement());
			if (value % failFrequency == 0) {
				throw new RuntimeException("Planned");
			}
		}

		public int getCount() {
			return count.get();
		}
	}

}
