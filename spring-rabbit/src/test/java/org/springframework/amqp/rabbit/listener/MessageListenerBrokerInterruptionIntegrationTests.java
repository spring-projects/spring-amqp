/*
 * Copyright 2010-2013 the original author or authors.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.admin.QueueInfo;
import org.springframework.amqp.rabbit.admin.RabbitBrokerAdmin;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerPanic;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.EnvironmentAvailable;

import com.rabbitmq.client.Channel;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @since 1.0
 *
 */
public class MessageListenerBrokerInterruptionIntegrationTests {

	private static Log logger = LogFactory.getLog(MessageListenerBrokerInterruptionIntegrationTests.class);

	// Ensure queue is durable, or it won't survive the broker restart
	private final Queue queue = new Queue("test.queue", true);

	private final int concurrentConsumers = 2;

	private final int messageCount = 60;

	private final int txSize = 1;

	private final boolean transactional = false;

	private final AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	private SimpleMessageListenerContainer container;

	@Rule
	public static EnvironmentAvailable environment = new EnvironmentAvailable("BROKER_INTEGRATION_TEST");

	/*
	 * Ensure broker dies if a test fails (otherwise the erl process might have to be killed manually)
	 */
	@Rule
	public static BrokerPanic panic = new BrokerPanic();

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue);

	private CachingConnectionFactory connectionFactory;

	private final RabbitBrokerAdmin brokerAdmin;

	public MessageListenerBrokerInterruptionIntegrationTests() throws Exception {
		FileUtils.deleteDirectory(new File("target/rabbitmq"));
		brokerIsRunning.setPort(BrokerTestUtils.getAdminPort());
		logger.debug("Setting up broker");
		brokerAdmin = BrokerTestUtils.getRabbitBrokerAdmin();
		panic.setBrokerAdmin(brokerAdmin);
		if (environment.isActive()) {
			brokerAdmin.startNode();
		}
	}

	@Before
	public void createConnectionFactory() {
		if (environment.isActive()) {
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setChannelCacheSize(concurrentConsumers);
			connectionFactory.setPort(BrokerTestUtils.getAdminPort());
			this.connectionFactory = connectionFactory;
		}
	}

	@After
	public void clear() throws Exception {
		if (environment.isActive()) {
			// Wait for broker communication to finish before trying to stop container
			Thread.sleep(300L);
			logger.debug("Shutting down at end of test");
			if (container != null) {
				container.shutdown();
			}
			brokerAdmin.stopNode();
			// Remove all trace of the durable queue...
			FileUtils.deleteDirectory(new File("target/rabbitmq"));

			if (this.connectionFactory != null) {
				this.connectionFactory.destroy();
			}
		}
	}

	@Test
	public void testListenerRecoversFromDeadBroker() throws Exception {

		List<QueueInfo> queues = brokerAdmin.getQueues();
		logger.info("Queues: " + queues);
		assertEquals(1, queues.size());
		assertTrue(queues.get(0).isDurable());

		RabbitTemplate template = new RabbitTemplate(connectionFactory);

		CountDownLatch latch = new CountDownLatch(messageCount);
		assertEquals("No more messages to receive before even sent!", messageCount, latch.getCount());
		container = createContainer(queue.getName(), new VanillaListener(latch), connectionFactory);
		for (int i = 0; i < messageCount; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}

		assertTrue("No more messages to receive before broker stopped", latch.getCount() > 0);
		brokerAdmin.stopBrokerApplication();
		assertTrue("No more messages to receive after broker stopped", latch.getCount() > 0);
		boolean waited = latch.await(500, TimeUnit.MILLISECONDS);
		assertFalse("Did not time out waiting for message", waited);

		container.stop();
		assertEquals(0, container.getActiveConsumerCount());

		brokerAdmin.startBrokerApplication();
		queues = brokerAdmin.getQueues();
		logger.info("Queues: " + queues);
		container.start();
		assertEquals(concurrentConsumers, container.getActiveConsumerCount());

		int timeout = Math.min(4 + messageCount / (4 * concurrentConsumers), 30);
		logger.debug("Waiting for messages with timeout = " + timeout + " (s)");
		waited = latch.await(timeout, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);

		assertNull(template.receiveAndConvert(queue.getName()));

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
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	public static class VanillaListener implements ChannelAwareMessageListener {

		private final CountDownLatch latch;

		public VanillaListener(CountDownLatch latch) {
			this.latch = latch;
		}

		public void onMessage(Message message, Channel channel) throws Exception {
			String value = new String(message.getBody());
			logger.debug("Receiving: " + value);
			latch.countDown();
		}
	}
}
