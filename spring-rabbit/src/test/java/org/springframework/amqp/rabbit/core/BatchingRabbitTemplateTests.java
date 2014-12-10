/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.amqp.rabbit.core;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.stubbing.answers.DoesNothing;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.support.BatchingStrategy;
import org.springframework.amqp.rabbit.core.support.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.StopWatch;

/**
 * @author Gary Russell
 * @since 1.4.1
 *
 */
public class BatchingRabbitTemplateTests {

	private static final String ROUTE = "test.queue";

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(ROUTE);

	private CachingConnectionFactory connectionFactory;

	private ThreadPoolTaskScheduler scheduler;

	@Before
	public void setup() {
		this.connectionFactory = new CachingConnectionFactory();
		this.connectionFactory.setHost("localhost");
		this.connectionFactory.setPort(BrokerTestUtils.getPort());
		scheduler = new ThreadPoolTaskScheduler();
		scheduler.setPoolSize(1);
		scheduler.initialize();
	}

	@Test
	public void testSimpleBatch() throws Exception {
		BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(2, Integer.MAX_VALUE, 30000);
		BatchingRabbitTemplate template = new BatchingRabbitTemplate(batchingStrategy, this.scheduler);
		template.setConnectionFactory(this.connectionFactory);
		MessageProperties props = new MessageProperties();
		Message message = new Message("foo".getBytes(), props);
		template.send("", ROUTE, message);
		message = new Message("bar".getBytes(), props);
		template.send("", ROUTE, message);
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("\u0000\u0000\u0000\u0003foo\u0000\u0000\u0000\u0003bar", new String(message.getBody()));
	}

	@Test
	public void testSimpleBatchTimeout() throws Exception {
		BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(2, Integer.MAX_VALUE, 50);
		BatchingRabbitTemplate template = new BatchingRabbitTemplate(batchingStrategy, this.scheduler);
		template.setConnectionFactory(this.connectionFactory);
		MessageProperties props = new MessageProperties();
		Message message = new Message("foo".getBytes(), props);
		template.send("", ROUTE, message);
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("foo", new String(message.getBody()));
	}

	@Test
	public void testSimpleBatchTimeoutMultiple() throws Exception {
		BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(2, Integer.MAX_VALUE, 50);
		BatchingRabbitTemplate template = new BatchingRabbitTemplate(batchingStrategy, this.scheduler);
		template.setConnectionFactory(this.connectionFactory);
		MessageProperties props = new MessageProperties();
		Message message = new Message("foo".getBytes(), props);
		template.send("", ROUTE, message);
		template.send("", ROUTE, message);
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("\u0000\u0000\u0000\u0003foo\u0000\u0000\u0000\u0003foo", new String(message.getBody()));
	}

	@Test
	public void testSimpleBatchBufferLimit() throws Exception {
		BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(2, 8, 50);
		BatchingRabbitTemplate template = new BatchingRabbitTemplate(batchingStrategy, this.scheduler);
		template.setConnectionFactory(this.connectionFactory);
		MessageProperties props = new MessageProperties();
		Message message = new Message("foo".getBytes(), props);
		template.send("", ROUTE, message);
		message = new Message("bar".getBytes(), props);
		template.send("", ROUTE, message);
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("foo", new String(message.getBody()));
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("bar", new String(message.getBody()));
	}

	@Test
	public void testSimpleBatchBufferLimitMultiple() throws Exception {
		BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(2, 15, 30000);
		BatchingRabbitTemplate template = new BatchingRabbitTemplate(batchingStrategy, this.scheduler);
		template.setConnectionFactory(this.connectionFactory);
		MessageProperties props = new MessageProperties();
		Message message = new Message("foo".getBytes(), props);
		template.send("", ROUTE, message);
		template.send("", ROUTE, message);
		message = new Message("bar".getBytes(), props);
		template.send("", ROUTE, message);
		template.send("", ROUTE, message);
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("\u0000\u0000\u0000\u0003foo\u0000\u0000\u0000\u0003foo", new String(message.getBody()));
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("\u0000\u0000\u0000\u0003bar\u0000\u0000\u0000\u0003bar", new String(message.getBody()));
	}

	@Test
	public void testSimpleBatchBiggerThanBufferLimit() throws Exception {
		BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(2, 2, 30000);
		BatchingRabbitTemplate template = new BatchingRabbitTemplate(batchingStrategy, this.scheduler);
		template.setConnectionFactory(this.connectionFactory);
		MessageProperties props = new MessageProperties();
		Message message = new Message("foo".getBytes(), props);
		template.send("", ROUTE, message);
		message = new Message("bar".getBytes(), props);
		template.send("", ROUTE, message);
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("foo", new String(message.getBody()));
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("bar", new String(message.getBody()));
	}

	@Test
	// existing buffered; new message bigger than bufferLimit; released immediately
	public void testSimpleBatchBiggerThanBufferLimitMultiple() throws Exception {
		BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(2, 6, 30000);
		BatchingRabbitTemplate template = new BatchingRabbitTemplate(batchingStrategy, this.scheduler);
		template.setConnectionFactory(this.connectionFactory);
		MessageProperties props = new MessageProperties();
		Message message = new Message("f".getBytes(), props);
		template.send("", ROUTE, message);
		message = new Message("bar".getBytes(), props);
		template.send("", ROUTE, message);
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("f", new String(message.getBody()));
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("bar", new String(message.getBody()));
	}

	@Test
	public void testSimpleBatchTwoEqualBufferLimit() throws Exception {
		BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(10, 14, 30000);
		BatchingRabbitTemplate template = new BatchingRabbitTemplate(batchingStrategy, this.scheduler);
		template.setConnectionFactory(this.connectionFactory);
		MessageProperties props = new MessageProperties();
		Message message = new Message("foo".getBytes(), props);
		template.send("", ROUTE, message);
		message = new Message("bar".getBytes(), props);
		template.send("", ROUTE, message);
		Thread.sleep(100);
		message = template.receive(ROUTE);
		assertNotNull(message);
		assertEquals("\u0000\u0000\u0000\u0003foo\u0000\u0000\u0000\u0003bar", new String(message.getBody()));
	}

	@Test
	public void testDebatchByContainer() throws Exception {
		final List<Message> received = new ArrayList<Message>();
		final CountDownLatch latch = new CountDownLatch(2);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactory);
		container.setQueueNames(ROUTE);
		container.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(Message message) {
				received.add(message);
				latch.countDown();
			}
		});
		container.setReceiveTimeout(100);
		container.afterPropertiesSet();
		container.start();
		try {
			BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(2, Integer.MAX_VALUE, 30000);
			BatchingRabbitTemplate template = new BatchingRabbitTemplate(batchingStrategy, this.scheduler);
			template.setConnectionFactory(this.connectionFactory);
			MessageProperties props = new MessageProperties();
			Message message = new Message("foo".getBytes(), props);
			template.send("", ROUTE, message);
			message = new Message("bar".getBytes(), props);
			template.send("", ROUTE, message);
			assertTrue(latch.await(10,  TimeUnit.SECONDS));
			assertEquals(2, received.size());
			assertEquals("foo", new String(received.get(0).getBody()));
			assertEquals(3, received.get(0).getMessageProperties().getContentLength());
			assertEquals("bar", new String(received.get(1).getBody()));
			assertEquals(3, received.get(0).getMessageProperties().getContentLength());
		}
		finally {
			container.stop();
		}
	}

	@Test
	public void testDebatchByContainerPerformance() throws Exception {
		final List<Message> received = new ArrayList<Message>();
		int count = 100000;
		final CountDownLatch latch = new CountDownLatch(count);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactory);
		container.setQueueNames(ROUTE);
		container.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(Message message) {
				received.add(message);
				latch.countDown();
			}
		});
		container.setReceiveTimeout(100);
		container.setPrefetchCount(1000);
		container.setTxSize(1000);
		container.afterPropertiesSet();
		container.start();
		try {
			BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(1000, Integer.MAX_VALUE, 30000);
			BatchingRabbitTemplate template = new BatchingRabbitTemplate(batchingStrategy, this.scheduler);
//			RabbitTemplate template = new RabbitTemplate();
			template.setConnectionFactory(this.connectionFactory);
			MessageProperties props = new MessageProperties();
			props.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
			Message message = new Message(new byte[256], props);
			StopWatch watch = new StopWatch();
			watch.start();
			for (int i = 0; i < count; i++) {
				template.send("", ROUTE, message);
			}
			assertTrue(latch.await(60,  TimeUnit.SECONDS));
			watch.stop();
			System.out.println(watch.getTotalTimeMillis());
			assertEquals(count, received.size());
		}
		finally {
			container.stop();
		}
	}

	@Test
	public void testDebatchByContainerBadMessageRejected() throws Exception {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactory);
		container.setQueueNames(ROUTE);
		container.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(Message message) {
			}
		});
		container.setReceiveTimeout(100);
		ConditionalRejectingErrorHandler errorHandler = new ConditionalRejectingErrorHandler();
		container.setErrorHandler(errorHandler);
		container.afterPropertiesSet();
		container.start();
		Log logger = spy(TestUtils.getPropertyValue(errorHandler, "logger", Log.class));
		new DirectFieldAccessor(errorHandler).setPropertyValue("logger", logger);
		when(logger.isWarnEnabled()).thenReturn(true);
		doAnswer(new DoesNothing()).when(logger).warn(anyString(), any(Throwable.class));
		try {
			RabbitTemplate template = new RabbitTemplate();
			template.setConnectionFactory(this.connectionFactory);
			MessageProperties props = new MessageProperties();
			props.getHeaders().put(MessageProperties.SPRING_BATCH_FORMAT, MessageProperties.BATCH_FORMAT_LENGTH_HEADER4);
			Message message = new Message("\u0000\u0000\u0000\u0004foo".getBytes(), props);
			template.send("", ROUTE, message);
			Thread.sleep(1000);
			ArgumentCaptor<Object> arg1 = ArgumentCaptor.forClass(Object.class);
			ArgumentCaptor<Throwable> arg2 = ArgumentCaptor.forClass(Throwable.class);
			verify(logger, times(2)).warn(arg1.capture(), arg2.capture()); // CRE logs 2 WARNs ensure the message was rejected
			assertThat(arg2.getValue().getMessage(), containsString("Bad batched message received"));
		}
		finally {
			container.stop();
		}
	}

}
