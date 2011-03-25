package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;
import org.springframework.retry.interceptor.MethodArgumentsKeyGenerator;
import org.springframework.retry.interceptor.MethodInvocationRecoverer;
import org.springframework.retry.interceptor.NewMethodArgumentsIdentifier;
import org.springframework.retry.interceptor.StatefulRetryOperationsInterceptor;
import org.springframework.retry.support.RetryTemplate;

public class MessageListenerContainerRetryIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerContainerRetryIntegrationTests.class);

	private static Queue queue = new Queue("test.queue");

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueue(queue);

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.INFO, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class);

	private RabbitTemplate createTemplate(int concurrentConsumers) {
		RabbitTemplate template = new RabbitTemplate();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
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

	public void doTestStatefulRetry(int messageCount, int txSize, int failFrequency, int concurrentConsumers) throws Exception {

		int failedMessageCount = messageCount / failFrequency + (messageCount % failFrequency == 0 ? 0 : 1);
		RabbitTemplate template = createTemplate(concurrentConsumers);

		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), new Integer(i), new MessagePostProcessor() {
				// There is no message id by default
				public Message postProcessMessage(Message message) throws AmqpException {
					message.getMessageProperties().setMessageId(UUID.randomUUID().toString());
					return message;
				}
			});
		}

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		PojoListener listener = new PojoListener(failFrequency);
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.setChannelTransacted(true);
		container.setTxSize(txSize);
		container.setConcurrentConsumers(concurrentConsumers);

		StatefulRetryOperationsInterceptor retryInterceptor = new StatefulRetryOperationsInterceptor();
		retryInterceptor.setRetryOperations(new RetryTemplate());
		retryInterceptor.setNewItemIdentifier(new NewMethodArgumentsIdentifier() {
			public boolean isNew(Object[] args) {
				Message message = (Message) args[1];
				return !message.getMessageProperties().isRedelivered();
			}
		});
		final CountDownLatch latch = new CountDownLatch(failedMessageCount);
		retryInterceptor.setRecoverer(new MethodInvocationRecoverer<Object>() {
			public Object recover(Object[] args, Throwable cause) {
				logger.info("Recovered: " + Arrays.asList(args));
				latch.countDown();
				return null;
			}
		});
		retryInterceptor.setKeyGenerator(new MethodArgumentsKeyGenerator() {
			public Object getKey(Object[] args) {
				Message message = (Message) args[1];
				logger.info("Key: " + message.getMessageProperties().getMessageId());
				return message.getMessageProperties().getMessageId();
			}
		});
		container.setAdviceChain(new Advice[] { retryInterceptor });

		container.setQueueNames(queue.getName());
		container.afterPropertiesSet();
		container.start();

		try {

			int timeout = Math.min(1 + messageCount / concurrentConsumers, 30);

			logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
			boolean waited = latch.await(timeout, TimeUnit.SECONDS);
			logger.info("All messages recovered: " + waited);
			assertEquals(concurrentConsumers, container.getActiveConsumerCount());
			assertTrue("Timed out waiting for messages", waited);

			assertEquals(concurrentConsumers, container.getActiveConsumerCount());

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
			logger.debug(value + count.getAndIncrement());
			if (value % failFrequency == 0) {
				throw new RuntimeException("Planned");
			}
		}

		public int getCount() {
			return count.get();
		}
	}

}
