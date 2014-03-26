/*
 * Copyright 2010-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.ErrorHandler;

import com.rabbitmq.client.Channel;

/**
 * @author Dave Syer
 * @author Gunar Hillert
 * @author Gary Russell
 * @since 1.0
 *
 */
public class MessageListenerContainerErrorHandlerIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerContainerErrorHandlerIntegrationTests.class);

	private static Queue queue = new Queue("test.queue");

	// Mock error handler
	private final ErrorHandler errorHandler = mock(ErrorHandler.class);

	private volatile CountDownLatch errorsHandled;

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue);

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.INFO, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class,
			MessageListenerContainerErrorHandlerIntegrationTests.class);

	@Before
	public void setUp() {
		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				errorsHandled.countDown();
				return null;
			}
		}).when(errorHandler).handleError(any(Throwable.class));
	}

	@Test // AMQP-385
	public void testErrorHandlerThrowsARADRE() throws Exception {
		RabbitTemplate template = this.createTemplate(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setQueues(queue);
		final CountDownLatch messageReceived = new CountDownLatch(1);
		final CountDownLatch spiedQLogger = new CountDownLatch(1);
		final CountDownLatch errorHandled = new CountDownLatch(1);
		container.setErrorHandler(new ErrorHandler() {

			@Override
			public void handleError(Throwable t) {
				errorHandled.countDown();
				throw new AmqpRejectAndDontRequeueException("foo", t);
			}
		});
		container.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(Message message) {
				try {
					messageReceived.countDown();
					spiedQLogger.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				throw new RuntimeException("bar");
			}
		});
		container.start();
		Log logger = spy(TestUtils.getPropertyValue(container, "logger", Log.class));
		new DirectFieldAccessor(container).setPropertyValue("logger", logger);
		when(logger.isWarnEnabled()).thenReturn(true);
		template.convertAndSend(queue.getName(), "baz");
		assertTrue(messageReceived.await(10, TimeUnit.SECONDS));
		Object consumer = TestUtils.getPropertyValue(container, "consumers", Map.class)
				.keySet().iterator().next();
		Log qLogger = spy(TestUtils.getPropertyValue(consumer, "logger", Log.class));
		new DirectFieldAccessor(consumer).setPropertyValue("logger", qLogger);
		when(qLogger.isDebugEnabled()).thenReturn(true);
		spiedQLogger.countDown();
		assertTrue(errorHandled.await(10, TimeUnit.SECONDS));
		container.stop();
		verify(logger, never()).warn(contains("Consumer raised exception"), any(Throwable.class));
		verify(qLogger).debug(contains("Rejecting messages (requeue=false)"));
	}

	@Test
	public void testErrorHandlerInvokeExceptionFromPojo() throws Exception {
		int messageCount = 3;
		CountDownLatch latch = new CountDownLatch(messageCount);
		doTest(messageCount, errorHandler, latch, new MessageListenerAdapter(new PojoThrowingExceptionListener(latch,
				new Exception("Pojo exception"))));
	}

	@Test
	public void testErrorHandlerInvokeRuntimeExceptionFromPojo() throws Exception {
		int messageCount = 3;
		CountDownLatch latch = new CountDownLatch(messageCount);
		doTest(messageCount, errorHandler, latch, new MessageListenerAdapter(new PojoThrowingExceptionListener(latch,
				new RuntimeException("Pojo runtime exception"))));
	}

	@Test
	public void testErrorHandlerListenerExecutionFailedExceptionFromListener() throws Exception {
		int messageCount = 3;
		CountDownLatch latch = new CountDownLatch(messageCount);
		doTest(messageCount, errorHandler, latch, new ThrowingExceptionListener(latch,
				new ListenerExecutionFailedException("Listener throws specific runtime exception", null)));
	}

	@Test
	public void testErrorHandlerRegularRuntimeExceptionFromListener() throws Exception {
		int messageCount = 3;
		CountDownLatch latch = new CountDownLatch(messageCount);
		doTest(messageCount, errorHandler, latch, new ThrowingExceptionListener(latch, new RuntimeException(
				"Listener runtime exception")));
	}

	@Test
	public void testErrorHandlerInvokeExceptionFromChannelAwareListener() throws Exception {
		int messageCount = 3;
		CountDownLatch latch = new CountDownLatch(messageCount);
		doTest(messageCount, errorHandler, latch, new ThrowingExceptionChannelAwareListener(latch, new Exception(
				"Channel aware listener exception")));
	}

	@Test
	public void testErrorHandlerInvokeRuntimeExceptionFromChannelAwareListener() throws Exception {
		int messageCount = 3;
		CountDownLatch latch = new CountDownLatch(messageCount);
		doTest(messageCount, errorHandler, latch, new ThrowingExceptionChannelAwareListener(latch,
				new RuntimeException("Channel aware listener runtime exception")));
	}

	public void doTest(int messageCount, ErrorHandler errorHandler, CountDownLatch latch, Object listener)
			throws Exception {
		this.errorsHandled = new CountDownLatch(messageCount);
		int concurrentConsumers = 1;
		RabbitTemplate template = createTemplate(concurrentConsumers);

		// Send messages to the queue
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setMessageListener(listener);
		container.setAcknowledgeMode(AcknowledgeMode.NONE);
		container.setChannelTransacted(false);
		container.setConcurrentConsumers(concurrentConsumers);

		container.setPrefetchCount(messageCount);
		container.setTxSize(messageCount);
		container.setQueueNames(queue.getName());
		container.setErrorHandler(errorHandler);
		container.afterPropertiesSet();
		container.start();

		try {
			boolean waited = latch.await(5000, TimeUnit.MILLISECONDS);
			if (messageCount > 1) {
				assertTrue("Expected to receive all messages before stop", waited);
			}

			assertTrue("Not enough error handling, remaining:" + this.errorsHandled.getCount(),
					this.errorsHandled.await(10, TimeUnit.SECONDS));
			assertNull(template.receiveAndConvert(queue.getName()));
		}
		finally {
			container.shutdown();
		}

		((DisposableBean) template.getConnectionFactory()).destroy();
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

	// ///////////////
	// Helper classes
	// ///////////////
	public static class PojoThrowingExceptionListener {
		private final CountDownLatch latch;
		private final Throwable exception;

		public PojoThrowingExceptionListener(CountDownLatch latch, Throwable exception) {
			this.latch = latch;
			this.exception = exception;
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
		private final CountDownLatch latch;
		private final RuntimeException exception;

		public ThrowingExceptionListener(CountDownLatch latch, RuntimeException exception) {
			this.latch = latch;
			this.exception = exception;
		}

		@Override
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

	public static class ThrowingExceptionChannelAwareListener implements ChannelAwareMessageListener {
		private final CountDownLatch latch;
		private final Exception exception;

		public ThrowingExceptionChannelAwareListener(CountDownLatch latch, Exception exception) {
			this.latch = latch;
			this.exception = exception;
		}

		@Override
		public void onMessage(Message message, Channel channel) throws Exception {
			try {
				String value = new String(message.getBody());
				logger.debug("Message in channel aware listener: " + value);
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
