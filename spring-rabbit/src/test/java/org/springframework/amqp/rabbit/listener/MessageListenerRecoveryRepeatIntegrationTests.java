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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerExecutionException;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.amqp.rabbit.test.RepeatProcessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.test.annotation.Repeat;

import com.rabbitmq.client.Channel;

/**
 * Long-running test created to facilitate profiling of SimpleMessageListenerContainer.
 *
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 *
 */
public class MessageListenerRecoveryRepeatIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerRecoveryRepeatIntegrationTests.class);

	private final Queue queue = new Queue("test.queue");

	private final Queue sendQueue = new Queue("test.send");

	private final int concurrentConsumers = 1;

	private final int messageCount = 2;

	private final int txSize = 1;

	private final boolean transactional = false;

	private final AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	private SimpleMessageListenerContainer container;

	@Rule
	public LogLevelAdjuster logLevels = new LogLevelAdjuster(Level.ERROR, RabbitTemplate.class,
			ConditionalRejectingErrorHandler.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class, MessageListenerRecoveryRepeatIntegrationTests.class);

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue.getName(), sendQueue.getName());

	@Rule
	public RepeatProcessor repeatProcessor = new RepeatProcessor();

	private CloseConnectionListener listener;

	private ConnectionFactory connectionFactory;

	@Before
	public void init() {
		if (!repeatProcessor.isInitialized()) {
			logger.info("Initializing at start of test");
			connectionFactory = createConnectionFactory();
			listener = new CloseConnectionListener();
		}
	}

	@After
	public void clear() throws Exception {
		if (repeatProcessor.isFinalizing()) {
			// Wait for broker communication to finish before trying to stop container
			Thread.sleep(300L);
			logger.info("Shutting down at end of test");
			if (container != null) {
				container.shutdown();
			}
			if (connectionFactory != null) {
				((DisposableBean) connectionFactory).destroy();
			}
			this.brokerIsRunning.removeTestQueues();
		}
	}

	@Test
	@Repeat(1000)
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
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

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
		container.setTxSize(txSize);
		container.setPrefetchCount(txSize);
		container.setConcurrentConsumers(concurrentConsumers);
		container.setChannelTransacted(transactional);
		container.setAcknowledgeMode(acknowledgeMode);
		container.setTaskExecutor(Executors.newFixedThreadPool(concurrentConsumers));
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	private static class CloseConnectionListener implements ChannelAwareMessageListener {

		private final AtomicBoolean failed = new AtomicBoolean(false);

		private CountDownLatch latch;

		CloseConnectionListener() {
			super();
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
