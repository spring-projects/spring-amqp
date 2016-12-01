/*
 * Copyright 2002-2016 the original author or authors.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

/**
 * @author Mark Fisher
 * @author Gunnar Hillert
 * @author Gary Russell
 */
public class MessageListenerContainerMultipleQueueIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerContainerMultipleQueueIntegrationTests.class);

	private static Queue queue1 = new Queue("test.queue.1");

	private static Queue queue2 = new Queue("test.queue.2");

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue1.getName(), queue2.getName());

	@Rule
	public LogLevelAdjuster logLevels = new LogLevelAdjuster(Level.INFO, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class);

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@After
	public void tearDown() {
		this.brokerIsRunning.removeTestQueues();
	}

	@Test
	public void testMultipleQueues() {
		doTest(1, container -> container.setQueues(queue1, queue2));
	}

	@Test
	public void testMultipleQueueNames() {
		doTest(1, container -> container.setQueueNames(queue1.getName(), queue2.getName()));
	}

	@Test
	public void testMultipleQueuesWithConcurrentConsumers() {
		doTest(3, container -> container.setQueues(queue1, queue2));
	}

	@Test
	public void testMultipleQueueNamesWithConcurrentConsumers() {
		doTest(3, container -> container.setQueueNames(queue1.getName(), queue2.getName()));
	}


	private void doTest(int concurrentConsumers, ContainerConfigurer configurer) {
		int messageCount = 10;
		RabbitTemplate template = new RabbitTemplate();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
		SimpleMessageConverter messageConverter = new SimpleMessageConverter();
		messageConverter.setCreateMessageIds(true);
		template.setMessageConverter(messageConverter);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue1.getName(), new Integer(i));
			template.convertAndSend(queue2.getName(), new Integer(i));
		}
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		final CountDownLatch latch = new CountDownLatch(messageCount * 2);
		PojoListener listener = new PojoListener(latch);
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.setChannelTransacted(true);
		container.setConcurrentConsumers(concurrentConsumers);
		configurer.configure(container);
		container.afterPropertiesSet();
		container.start();
		try {
			int timeout = Math.min(1 + messageCount / concurrentConsumers, 30);
			boolean waited = latch.await(timeout, TimeUnit.SECONDS);
			logger.info("All messages recovered: " + waited);
			assertEquals(concurrentConsumers, container.getActiveConsumerCount());
			assertTrue("Timed out waiting for messages", waited);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("unexpected interruption");
		}
		finally {
			container.shutdown();
			assertEquals(0, container.getActiveConsumerCount());
		}
		assertNull(template.receiveAndConvert(queue1.getName()));
		assertNull(template.receiveAndConvert(queue2.getName()));

		connectionFactory.destroy();
	}

	@FunctionalInterface
	private interface ContainerConfigurer {
		void configure(SimpleMessageListenerContainer container);
	}


	@SuppressWarnings("unused")
	private static class PojoListener {

		private final AtomicInteger count = new AtomicInteger();

		private final CountDownLatch latch;

		PojoListener(CountDownLatch latch) {
			this.latch = latch;
		}

		public void handleMessage(int value) throws Exception {
			logger.debug(value + ":" + count.getAndIncrement());
			latch.countDown();
		}

		public int getCount() {
			return count.get();
		}
	}

}
