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

import com.rabbitmq.client.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.DisposableBean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 *
 * @since 1.0
 *
 */
@RabbitAvailable(queues = MessageListenerTxSizeIntegrationTests.TEST_QUEUE)
@LogLevels(level = "ERROR", classes = {RabbitTemplate.class,
		SimpleMessageListenerContainer.class, BlockingQueueConsumer.class})
public class MessageListenerTxSizeIntegrationTests {

	public static final String TEST_QUEUE = "test.queue.MessageListenerTxSizeIntegrationTests";

	private static Log logger = LogFactory.getLog(MessageListenerTxSizeIntegrationTests.class);

	private final Queue queue = new Queue(TEST_QUEUE);

	private final RabbitTemplate template = new RabbitTemplate();

	private final int concurrentConsumers = 1;

	private final int messageCount = 12;

	private final int txSize = 4;

	private boolean transactional = true;

	private SimpleMessageListenerContainer container;

	@BeforeEach
	public void createConnectionFactory() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
	}

	@AfterEach
	public void clear() throws Exception {
		// Wait for broker communication to finish before trying to stop container
		Thread.sleep(300L);
		logger.debug("Shutting down at end of test");
		if (container != null) {
			container.shutdown();
		}

		((DisposableBean) template.getConnectionFactory()).destroy();
	}

	@Test
	public void testListenerTransactionalSunnyDay() throws Exception {
		transactional = true;
		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(new TestListener(latch, false));
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		int timeout = Math.min(1 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertThat(waited).as("Timed out waiting for message").isTrue();
		assertThat(template.receiveAndConvert(queue.getName())).isNull();
	}

	@Test
	public void testListenerTransactionalFails() throws Exception {
		transactional = true;
		CountDownLatch latch = new CountDownLatch(messageCount);
		container = createContainer(new TestListener(latch, true));
		for (int i = 0; i < txSize; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		int timeout = Math.min(1 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertThat(waited).as("Timed out waiting for message").isTrue();
		assertThat(template.receiveAndConvert(queue.getName())).isNull();
	}

	private SimpleMessageListenerContainer createContainer(Object listener) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setQueueNames(queue.getName());
		container.setBatchSize(txSize);
		container.setPrefetchCount(txSize);
		container.setConcurrentConsumers(concurrentConsumers);
		container.setChannelTransacted(transactional);
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.setReceiveTimeout(10);
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	public class TestListener implements ChannelAwareMessageListener {

		private final ThreadLocal<Integer> count = new ThreadLocal<>();

		private final CountDownLatch latch;

		private final boolean fail;

		public TestListener(CountDownLatch latch, boolean fail) {
			this.latch = latch;
			this.fail = fail;
		}

		public void handleMessage(String value) {
		}

		@Override
		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			try {
				logger.debug("Received: " + value);
				if (count.get() == null) {
					count.set(1);
				}
				else {
					count.set(count.get() + 1);
				}
				if (count.get() == txSize && fail) {
					logger.debug("Failing: " + value);
					count.set(0);
					throw new RuntimeException("Planned");
				}
			}
			finally {
				latch.countDown();
			}
		}

	}

}
