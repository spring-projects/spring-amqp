/*
 * Copyright 2002-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.listener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.listener.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.ErrorHandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Dave Syer
 * @author Gunar Hillert
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.0
 *
 */
@RabbitAvailable(MessageListenerContainerErrorHandlerIntegrationTests.QUEUE_NAME)
public class MessageListenerContainerErrorHandlerIntegrationTests {

	public static final String QUEUE_NAME = "test.queue.MessageListenerContainerErrorHandlerIntegrationTests";

	private static Log LOGGER = LogFactory.getLog(MessageListenerContainerErrorHandlerIntegrationTests.class);

	private static Queue QUEUE = new Queue(QUEUE_NAME);

	// Mock error handler
	private final ErrorHandler errorHandler = mock(ErrorHandler.class);

	private volatile CountDownLatch errorsHandled;

	@BeforeEach
	public void setUp() {
		willAnswer(invocation -> {
			errorsHandled.countDown();
			return null;
		}).given(errorHandler).handleError(any(Throwable.class));
	}

	@Test // AMQP-385
	public void testErrorHandlerThrowsARADRE() throws Exception {
		RabbitTemplate template = this.createTemplate(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setQueues(QUEUE);
		container.setReceiveTimeout(10);
		final CountDownLatch messageReceived = new CountDownLatch(1);
		final CountDownLatch spiedQLogger = new CountDownLatch(1);
		final CountDownLatch errorHandled = new CountDownLatch(1);
		container.setErrorHandler(t -> {
			errorHandled.countDown();
			throw new AmqpRejectAndDontRequeueException("foo", t);
		});
		container.setMessageListener(message -> {
			try {
				messageReceived.countDown();
				spiedQLogger.await(10, TimeUnit.SECONDS);
			}
			catch (@SuppressWarnings("unused") InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			throw new RuntimeException("bar");
		});
		container.setReceiveTimeout(50);
		container.start();
		Log logger = spy(TestUtils.<Log>propertyValue(container, "logger"));
		willReturn(true).given(logger).isWarnEnabled();
		new DirectFieldAccessor(container).setPropertyValue("logger", logger);
		template.convertAndSend(QUEUE.getName(), "baz");
		assertThat(messageReceived.await(10, TimeUnit.SECONDS)).isTrue();
		spiedQLogger.countDown();
		assertThat(errorHandled.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		verify(logger, never()).warn(contains("Consumer raised exception"), any(Throwable.class));
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
	public void testRejectingErrorHandlerSimpleAuto() throws Exception {
		RabbitTemplate template = createTemplate(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setReceiveTimeout(50);
		testRejectingErrorHandler(template, container);
	}

	@Test
	public void testRejectingErrorHandlerSimpleManual() throws Exception {
		RabbitTemplate template = createTemplate(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setReceiveTimeout(50);
		container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
		testRejectingErrorHandler(template, container);
	}

	@Test
	public void testRejectingErrorHandlerDirectAuto() throws Exception {
		RabbitTemplate template = createTemplate(1);
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(template.getConnectionFactory());
		testRejectingErrorHandler(template, container);
	}

	@Test
	public void testRejectingErrorHandlerDirectManual() throws Exception {
		RabbitTemplate template = createTemplate(1);
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(template.getConnectionFactory());
		container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
		testRejectingErrorHandler(template, container);
	}

	private void testRejectingErrorHandler(RabbitTemplate template, AbstractMessageListenerContainer container)
			throws Exception {
		MessageListenerAdapter messageListener = new MessageListenerAdapter();
		messageListener.setDelegate(new Object());
		container.setMessageListener(messageListener);

		RabbitAdmin admin = new RabbitAdmin(template.getConnectionFactory());
		Queue queueForTest = QueueBuilder.nonDurable("")
				.autoDelete()
				.withArgument("x-dead-letter-exchange", "test.DLE")
				.build();
		String testQueueName = admin.declareQueue(queueForTest);
		// Create a DeadLetterExchange and bind a queue to it with the original routing key
		DirectExchange dle = new DirectExchange("test.DLE", false, true);
		admin.declareExchange(dle);
		Queue dlq = new AnonymousQueue();
		admin.declareQueue(dlq);
		admin.declareBinding(BindingBuilder.bind(dlq).to(dle).with(testQueueName));

		container.setQueueNames(testQueueName);
		container.afterPropertiesSet();
		container.start();

		Message message = MessageBuilder.withBody("foo".getBytes())
				.setContentType("text/plain")
				.setContentEncoding("junk")
				.build();
		template.send("", testQueueName, message);

		// can't use timed receive, queue will be deleted
		Message rejected = await("Message did not arrive in DLQ")
				.until(() -> template.receive(dlq.getName()), msg -> msg != null);
		assertThat(new String(rejected.getBody())).isEqualTo("foo");

		// Verify that the exception strategy has access to the message
		final AtomicReference<Message> failed = new AtomicReference<Message>();
		ConditionalRejectingErrorHandler eh = new ConditionalRejectingErrorHandler(t -> {
			if (t instanceof ListenerExecutionFailedException exception) {
				failed.set(exception.getFailedMessage());
			}
			return t instanceof ListenerExecutionFailedException
					&& t.getCause() instanceof MessageConversionException;
		});
		container.setErrorHandler(eh);

		template.send("", testQueueName, message);

		rejected = await("Message did not arrive in DLQ")
				.until(() -> template.receive(dlq.getName()), msg -> msg != null);
		assertThat(new String(rejected.getBody())).isEqualTo("foo");
		assertThat(failed.get()).isNotNull();

		container.stop();

		Exception e = new ListenerExecutionFailedException("foo", new MessageConversionException("bar"),
				new Message("".getBytes(), new MessageProperties()));
		try {
			eh.handleError(e);
			fail("expected exception");
		}
		catch (AmqpRejectAndDontRequeueException aradre) {
			assertThat(aradre.getCause()).isSameAs(e);
		}
		e = new ListenerExecutionFailedException("foo", new MessageConversionException("bar",
				new AmqpRejectAndDontRequeueException("baz")), mock(Message.class));
		eh.handleError(e);
		((DisposableBean) template.getConnectionFactory()).destroy();
	}

	public void doTest(int messageCount, ErrorHandler eh, CountDownLatch latch, MessageListener listener)
			throws Exception {
		this.errorsHandled = new CountDownLatch(messageCount);
		int concurrentConsumers = 1;
		RabbitTemplate template = createTemplate(concurrentConsumers);

		// Send messages to the queue
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(QUEUE.getName(), i + "foo");
		}

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setMessageListener(listener);
		container.setAcknowledgeMode(AcknowledgeMode.NONE);
		container.setChannelTransacted(false);
		container.setConcurrentConsumers(concurrentConsumers);

		container.setPrefetchCount(messageCount);
		container.setBatchSize(messageCount);
		container.setQueueNames(QUEUE.getName());
		container.setErrorHandler(eh);
		container.setReceiveTimeout(50);
		container.afterPropertiesSet();
		container.start();

		try {
			boolean waited = latch.await(5000, TimeUnit.MILLISECONDS);
			if (messageCount > 1) {
				assertThat(waited).as("Expected to receive all messages before stop").isTrue();
			}

			assertThat(this.errorsHandled.await(10, TimeUnit.SECONDS)).as("Not enough error handling, remaining:" + this.errorsHandled.getCount()).isTrue();
			assertThat(template.receiveAndConvert(QUEUE.getName())).isNull();
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
				LOGGER.debug("Message in pojo: " + value);
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
				LOGGER.debug("Message in listener: " + value);
				try {
					Thread.sleep(100L);
				}
				catch (@SuppressWarnings("unused") InterruptedException e) {
					Thread.currentThread().interrupt();
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
				LOGGER.debug("Message in channel aware listener: " + value);
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
