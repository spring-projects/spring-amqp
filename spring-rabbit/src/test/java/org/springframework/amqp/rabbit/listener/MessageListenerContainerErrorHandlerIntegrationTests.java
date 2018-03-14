/*
 * Copyright 2002-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.ErrorHandler;

import com.rabbitmq.client.Channel;

/**
 * @author Dave Syer
 * @author Gunar Hillert
 * @author Gary Russell
 * @author Artem Bilan
 *
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
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue.getName());

	@Before
	public void setUp() {
		doAnswer(invocation -> {
			errorsHandled.countDown();
			return null;
		}).when(errorHandler).handleError(any(Throwable.class));
	}

	@After
	public void tearDown() {
		this.brokerIsRunning.removeTestQueues();
	}

	@Test // AMQP-385
	public void testErrorHandlerThrowsARADRE() throws Exception {
		RabbitTemplate template = this.createTemplate(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setQueues(queue);
		final CountDownLatch messageReceived = new CountDownLatch(1);
		final CountDownLatch spiedQLogger = new CountDownLatch(1);
		final CountDownLatch errorHandled = new CountDownLatch(1);
		container.setErrorHandler(t -> {
			errorHandled.countDown();
			throw new AmqpRejectAndDontRequeueException("foo", t);
		});
		container.setMessageListener((MessageListener) message -> {
			try {
				messageReceived.countDown();
				spiedQLogger.await(10, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			throw new RuntimeException("bar");
		});
		container.setReceiveTimeout(50);
		container.start();
		Log logger = spy(TestUtils.getPropertyValue(container, "logger", Log.class));
		doReturn(true).when(logger).isWarnEnabled();
		new DirectFieldAccessor(container).setPropertyValue("logger", logger);
		template.convertAndSend(queue.getName(), "baz");
		assertTrue(messageReceived.await(10, TimeUnit.SECONDS));
		Object consumer = TestUtils.getPropertyValue(container, "consumers", Set.class)
				.iterator().next();
		Log qLogger = spy(TestUtils.getPropertyValue(consumer, "logger", Log.class));
		doReturn(true).when(qLogger).isDebugEnabled();
		new DirectFieldAccessor(consumer).setPropertyValue("logger", qLogger);
		spiedQLogger.countDown();
		assertTrue(errorHandled.await(10, TimeUnit.SECONDS));
		container.stop();
		verify(logger, never()).warn(contains("Consumer raised exception"), any(Throwable.class));
		verify(qLogger).debug(contains("Rejecting messages (requeue=false)"));
		((DisposableBean) template.getConnectionFactory()).destroy();
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
				new ListenerExecutionFailedException("Listener throws specific runtime exception", null,
						mock(Message.class))));
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

	@Test
	public void testRejectingErrorHandler() throws Exception {
		RabbitTemplate template = createTemplate(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		MessageListenerAdapter messageListener = new MessageListenerAdapter();
		messageListener.setDelegate(new Object());
		container.setMessageListener(messageListener);

		RabbitAdmin admin = new RabbitAdmin(template.getConnectionFactory());
		Queue queue = QueueBuilder.nonDurable("")
				.autoDelete()
				.withArgument("x-dead-letter-exchange", "test.DLE")
				.build();
		String testQueueName = admin.declareQueue(queue);
		// Create a DeadLetterExchange and bind a queue to it with the original routing key
		DirectExchange dle = new DirectExchange("test.DLE", false, true);
		admin.declareExchange(dle);
		Queue dlq = new AnonymousQueue();
		admin.declareQueue(dlq);
		admin.declareBinding(BindingBuilder.bind(dlq).to(dle).with(testQueueName));

		container.setQueueNames(testQueueName);
		container.setReceiveTimeout(50);
		container.afterPropertiesSet();
		container.start();

		Message message = MessageBuilder.withBody("foo".getBytes())
				.setContentType("text/plain")
				.setContentEncoding("junk")
				.build();
		template.send("", testQueueName, message);

		Message rejected = template.receive(dlq.getName());
		int n = 0;
		while (n++ < 100 && rejected == null) {
			Thread.sleep(100);
			rejected = template.receive(dlq.getName());
		}
		assertTrue("Message did not arrive in DLQ", n < 100);
		assertEquals("foo", new String(rejected.getBody()));


		// Verify that the exception strategy has access to the message
		final AtomicReference<Message> failed = new AtomicReference<Message>();
		ConditionalRejectingErrorHandler eh = new ConditionalRejectingErrorHandler(t -> {
			if (t instanceof ListenerExecutionFailedException) {
				failed.set(((ListenerExecutionFailedException) t).getFailedMessage());
			}
			return t instanceof ListenerExecutionFailedException
					&& t.getCause() instanceof MessageConversionException;
		});
		container.setErrorHandler(eh);

		template.send("", testQueueName, message);

		rejected = template.receive(dlq.getName());
		n = 0;
		while (n++ < 100 && rejected == null) {
			Thread.sleep(100);
			rejected = template.receive(dlq.getName());
		}
		assertTrue("Message did not arrive in DLQ", n < 100);
		assertEquals("foo", new String(rejected.getBody()));
		assertNotNull(failed.get());

		container.stop();

		Exception e = new ListenerExecutionFailedException("foo", new MessageConversionException("bar"),
				mock(Message.class));
		try {
			eh.handleError(e);
			fail("expected exception");
		}
		catch (AmqpRejectAndDontRequeueException aradre) {
			assertSame(e, aradre.getCause());
		}
		e = new ListenerExecutionFailedException("foo", new MessageConversionException("bar",
				new AmqpRejectAndDontRequeueException("baz")), mock(Message.class));
		eh.handleError(e);
		((DisposableBean) template.getConnectionFactory()).destroy();
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
		container.setReceiveTimeout(50);
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
		connectionFactory.setHost("localhost");
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
			}
			finally {
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
				}
				catch (InterruptedException e) {
					// Ignore this exception
				}
				throw exception;
			}
			finally {
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
				}
				catch (InterruptedException e) {
					// Ignore this exception
				}
				throw exception;
			}
			finally {
				latch.countDown();
			}
		}
	}

}
