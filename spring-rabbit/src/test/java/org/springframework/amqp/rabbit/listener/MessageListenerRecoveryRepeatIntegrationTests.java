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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rabbitmq.client.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerExecutionException;
import org.springframework.beans.factory.DisposableBean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Long-running test created to facilitate profiling of SimpleMessageListenerContainer.
 *
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 *
 */
@RabbitAvailable(queues = MessageListenerRecoveryRepeatIntegrationTests.TEST_QUEUE, purgeAfterEach = false)
@LogLevels(level = "ERROR", classes = { RabbitTemplate.class,
		ConditionalRejectingErrorHandler.class,
		SimpleMessageListenerContainer.class, BlockingQueueConsumer.class,
		MessageListenerRecoveryRepeatIntegrationTests.class })
@TestInstance(Lifecycle.PER_CLASS)
public class MessageListenerRecoveryRepeatIntegrationTests {

	public static final String TEST_QUEUE = "test.queue.MessageListenerRecoveryRepeatIntegrationTests";

	private static Log logger = LogFactory.getLog(MessageListenerRecoveryRepeatIntegrationTests.class);

	private final Queue queue = new Queue(TEST_QUEUE);

	private final int concurrentConsumers = 1;

	private final int messageCount = 2;

	private final int txSize = 1;

	private final boolean transactional = false;

	private final AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	private SimpleMessageListenerContainer container;

	private CloseConnectionListener listener;

	private ConnectionFactory connectionFactory;

	@BeforeEach
	public void init(RepetitionInfo info) {
		if (info.getCurrentRepetition() == 1) {
			logger.info("Initializing at start of test");
			connectionFactory = createConnectionFactory();
			listener = new CloseConnectionListener();
		}
	}

	@AfterEach
	public void clear(RepetitionInfo info) throws Exception {
		if (info.getCurrentRepetition() == info.getTotalRepetitions()) {
			// Wait for broker communication to finish before trying to stop container
			Thread.sleep(300L);
			logger.info("Shutting down at end of test");
			if (container != null) {
				container.shutdown();
			}
			if (connectionFactory != null) {
				((DisposableBean) connectionFactory).destroy();
			}
		}
	}

	@RepeatedTest(1000)
	public void testListenerRecoversFromClosedConnection() throws Exception {
		if (this.container == null) {
			this.container = createContainer(queue.getName(), listener, connectionFactory);
		}

		// logger.info("Testing...");

		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		CountDownLatch latch = new CountDownLatch(messageCount);
		listener.setLatch(latch);

		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		int timeout = Math.min(4 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		boolean waited = latch.await(timeout, TimeUnit.SECONDS);
		assertThat(waited).as("Timed out waiting for message").isTrue();

		assertThat(template.receiveAndConvert(queue.getName())).isNull();

	}

	private ConnectionFactory createConnectionFactory() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(concurrentConsumers);
		// connectionFactory.setPort(BrokerTestUtils.getTracerPort());
		connectionFactory.setPort(BrokerTestUtils.getPort());
		return connectionFactory;
	}

	private SimpleMessageListenerContainer createContainer(String queueName, Object listener,
			ConnectionFactory connectionFactory) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(listener));
		container.setQueueNames(queueName);
		container.setBatchSize(txSize);
		container.setPrefetchCount(txSize);
		container.setConcurrentConsumers(concurrentConsumers);
		container.setChannelTransacted(transactional);
		container.setAcknowledgeMode(acknowledgeMode);
		container.setTaskExecutor(Executors.newFixedThreadPool(concurrentConsumers));
		container.setReceiveTimeout(100L);
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	private static class CloseConnectionListener implements ChannelAwareMessageListener {

		private final AtomicBoolean failed = new AtomicBoolean(false);

		private CountDownLatch latch;

		CloseConnectionListener() {
		}

		public void setLatch(CountDownLatch latch) {
			this.latch = latch;
			failed.set(false);
		}

		@Override
		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			logger.info("Receiving: " + value);
			if (failed.compareAndSet(false, true)) {
				// intentional error (causes exception on connection thread):
				// channel.abort();
				// throw new RuntimeException("Planned");
				throw new FatalListenerExecutionException("Planned");
			}
			else {
				latch.countDown();
			}
		}
	}
}
