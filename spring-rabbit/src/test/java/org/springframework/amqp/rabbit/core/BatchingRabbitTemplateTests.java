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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.support.BatchingStrategy;
import org.springframework.amqp.rabbit.core.support.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

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

}
