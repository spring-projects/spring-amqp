/*
 * Copyright 2002-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

/**
 * @author Mark Fisher
 * @author Gunnar Hillert
 * @author Gary Russell
 */
@RabbitAvailable(queues = { MessageListenerContainerMultipleQueueIntegrationTests.TEST_QUEUE_1,
		MessageListenerContainerMultipleQueueIntegrationTests.TEST_QUEUE_2 })
@LogLevels(level = "INFO", classes = { RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class })
public class MessageListenerContainerMultipleQueueIntegrationTests {

	public static final String TEST_QUEUE_1 = "test.queue.1.MessageListenerContainerMultipleQueueIntegrationTests";

	public static final String TEST_QUEUE_2 = "test.queue.2.MessageListenerContainerMultipleQueueIntegrationTests";

	private static Log logger = LogFactory.getLog(MessageListenerContainerMultipleQueueIntegrationTests.class);

	private static Queue queue1 = new Queue(TEST_QUEUE_1);

	private static Queue queue2 = new Queue(TEST_QUEUE_2);

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
			template.convertAndSend(queue1.getName(), i);
			template.convertAndSend(queue2.getName(), i);
		}
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		final CountDownLatch latch = new CountDownLatch(messageCount * 2);
		PojoListener listener = new PojoListener(latch);
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.setChannelTransacted(true);
		container.setConcurrentConsumers(concurrentConsumers);
		container.setReceiveTimeout(10);
		configurer.configure(container);
		container.afterPropertiesSet();
		container.start();
		try {
			int timeout = Math.min(1 + messageCount / concurrentConsumers, 30);
			boolean waited = latch.await(timeout, TimeUnit.SECONDS);
			logger.info("All messages recovered: " + waited);
			assertThat(container.getActiveConsumerCount()).isEqualTo(concurrentConsumers);
			assertThat(waited).as("Timed out waiting for messages").isTrue();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("unexpected interruption");
		}
		finally {
			container.shutdown();
			assertThat(container.getActiveConsumerCount()).isEqualTo(0);
		}
		assertThat(template.receiveAndConvert(queue1.getName())).isNull();
		assertThat(template.receiveAndConvert(queue2.getName())).isNull();

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
