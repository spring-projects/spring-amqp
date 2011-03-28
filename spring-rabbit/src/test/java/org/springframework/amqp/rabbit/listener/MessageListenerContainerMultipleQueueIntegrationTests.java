/*
 * Copyright 2002-2011 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

/**
 * @author Mark Fisher
 */
public class MessageListenerContainerMultipleQueueIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerContainerMultipleQueueIntegrationTests.class);

	private static Queue queue1 = new Queue("test.queue.1");

	private static Queue queue2 = new Queue("test.queue.2");

	@Rule
	public BrokerRunning brokerIsRunningAndQueue1Empty = BrokerRunning.isRunningWithEmptyQueue(queue1);

	@Rule
	public BrokerRunning brokerIsRunningAndQueue2Empty = BrokerRunning.isRunningWithEmptyQueue(queue2);

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.INFO, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class);

	@Rule
	public ExpectedException exception = ExpectedException.none();


	@Test
	public void testMultipleQueues() {
		int messageCount = 10;
		int concurrentConsumers = 1;
		RabbitTemplate template = new RabbitTemplate();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setChannelCacheSize(1);
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
		container.setQueueNames(queue1.getName(), queue2.getName());
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
	}


	@SuppressWarnings("unused")
	private static class PojoListener {

		private AtomicInteger count = new AtomicInteger();

		private final CountDownLatch latch;

		public PojoListener(CountDownLatch latch) {
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
