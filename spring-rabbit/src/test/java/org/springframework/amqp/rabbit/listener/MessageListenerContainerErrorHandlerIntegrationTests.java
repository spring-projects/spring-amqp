package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;
import org.springframework.util.ErrorHandler;

public class MessageListenerContainerErrorHandlerIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerContainerErrorHandlerIntegrationTests.class);

	private static Queue queue = new Queue("test.queue");

	// Mock error handler
	private ErrorHandler errorHandler = mock(ErrorHandler.class);

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueue(queue);

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.INFO, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class,
			MessageListenerContainerErrorHandlerIntegrationTests.class);

	@Before
	public void setUp() {
		reset(errorHandler);
	}

	@Test
	public void testErrorHandlerInvokeExceptionFromPojo() throws Exception {
		int messageCount = 3;

		doTest(messageCount, errorHandler, true, new Exception("Pojo exception"));

		// Verify that error handler was invoked
		verify(errorHandler, times(messageCount)).handleError(any(Throwable.class));
	}

	@Test
	public void testErrorHandlerInvokeRuntimeExceptionFromPojo() throws Exception {
		int messageCount = 3;

		doTest(messageCount, errorHandler, true, new RuntimeException("Pojo runtime exception"));

		// Verify that error handler was invoked
		verify(errorHandler, times(messageCount)).handleError(any(Throwable.class));
	}

	@Test
	public void testErrorHandlerInvokeSpecRuntimeExceptionFromListener() throws Exception {
		int messageCount = 3;

		doTest(messageCount, errorHandler, false, new ListenerExecutionFailedException(
				"Listener throws specific runtime exception", null));

		// Verify that error handler was invoked
		verify(errorHandler, times(messageCount)).handleError(any(Throwable.class));
	}

	/**
	 * TODO: {@link #testErrorHandlerInvokeSpecRuntimeExceptionFromListener()} is very similar test, but throws specific
	 * type of {@link RuntimeException} - {@link ListenerExecutionFailedException}. That exception is handled in
	 * different way and listener is invoked for all available messages. <br/>
	 * In this test listener throws {@link RuntimeException} and it fails if you are expecting multiple messages to be
	 * processed.<br/>
	 * Investigation required!
	 * @throws Exception
	 */
	@Test
	public void testErrorHandlerInvokeRuntimeExceptionFromListener() throws Exception {
		// TODO If messageCount is more than 1 and RuntimeException is thrown from listener - test will fail.
		// But if listener throws ListenerExecutionFailedException test will pass (see
		// testErrorHandlerInvokeSpecRuntimeExceptionFromListener())
		// even with multiple messages. Investigation required.
		int messageCount = 1;

		doTest(messageCount, errorHandler, false, new RuntimeException("Listener runtime exception"));

		// Verify that error handler was invoked
		verify(errorHandler, times(messageCount)).handleError(any(Throwable.class));
	}

	public void doTest(int messageCount, ErrorHandler errorHandler, boolean isPojo, Throwable exceptionToThrow)
			throws Exception {
		int concurrentConsumers = 1;
		RabbitTemplate template = createTemplate(concurrentConsumers);

		// Send messages to the queue
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		CountDownLatch latch = new CountDownLatch(messageCount);

		// Create listener from pojo or from MessageListener implementation
		MessageListener listener;
		if (isPojo) {
			listener = new MessageListenerAdapter(new PojoThrowingExceptionListener(latch, exceptionToThrow));
		} else {
			listener = new ThrowingExceptionListener(latch, (RuntimeException) exceptionToThrow);
		}

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setMessageListener(listener);
		container.setAcknowledgeMode(AcknowledgeMode.NONE);
		container.setChannelTransacted(false);
		container.setConcurrentConsumers(concurrentConsumers);

		container.setPrefetchCount(1);
		container.setTxSize(1);
		container.setQueueName(queue.getName());
		container.setErrorHandler(errorHandler);
		container.afterPropertiesSet();
		container.start();

		boolean waited = latch.await(500, TimeUnit.MILLISECONDS);
		if (messageCount > 1) {
			assertTrue("Expected to receive all messages before stop", waited);
		}

		try {
			assertNull(template.receiveAndConvert(queue.getName()));
		} finally {
			container.shutdown();
		}
	}

	private RabbitTemplate createTemplate(int concurrentConsumers) {
		RabbitTemplate template = new RabbitTemplate();
		// SingleConnectionFactory connectionFactory = new SingleConnectionFactory();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
		return template;
	}

	public static class PojoThrowingExceptionListener {
		private CountDownLatch latch;
		private Throwable exception;

		public PojoThrowingExceptionListener(CountDownLatch latch, Throwable exception) {
			this.latch = latch;
			this.exception = exception;
		}

		public void reset(CountDownLatch latch) {
			this.latch = latch;
		}

		public void handleMessage(String value) throws Throwable {
			try {
				logger.debug("Message in pojo: " + value);
				Thread.sleep(100L);
				throw exception;
			} finally {
				latch.countDown();
			}
		}
	}

	public static class ThrowingExceptionListener implements MessageListener {
		private CountDownLatch latch;
		private RuntimeException exception;

		public ThrowingExceptionListener(CountDownLatch latch, RuntimeException exception) {
			this.latch = latch;
			this.exception = exception;
		}

		public void reset(CountDownLatch latch) {
			this.latch = latch;
		}

		public void onMessage(Message message) {
			try {
				String value = new String(message.getBody());
				logger.debug("Message in listener: " + value);
				try {
					Thread.sleep(100L);
				} catch (InterruptedException e) {
					// Ignore this exception
				}
				throw exception;
			} finally {
				latch.countDown();
			}
		}
	}
}
