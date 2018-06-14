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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.LongRunningIntegrationTest;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.2.1
 *
 */
public class SimpleMessageListenerContainerLongTests {

	private static final String QUEUE = "SimpleMessageListenerContainerLongTests.queue";

	private static final String QUEUE2 = "SimpleMessageListenerContainerLongTests.queue2";

	private static final String QUEUE3 = "SimpleMessageListenerContainerLongTests.queue3";

	private static final String QUEUE4 = "SimpleMessageListenerContainerLongTests.queue4";

	private final Log logger = LogFactory.getLog(SimpleMessageListenerContainerLongTests.class);

	@Rule
	public LongRunningIntegrationTest longTest = new LongRunningIntegrationTest();

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunningWithEmptyQueues(QUEUE, QUEUE2, QUEUE3, QUEUE4);

	@After
	public void tearDown() {
		this.brokerRunning.removeTestQueues();
	}

	@Test
	public void testChangeConsumerCount() throws Exception {
		testChangeConsumerCountGuts(false);
	}

	@Test
	public void testChangeConsumerCountTransacted() throws Exception {
		testChangeConsumerCountGuts(true);
	}

	private void testChangeConsumerCountGuts(boolean transacted) throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		try {
			container.setMessageListener(new MessageListenerAdapter(this));
			container.setQueueNames(QUEUE);
			container.setAutoStartup(false);
			container.setConcurrentConsumers(2);
			container.setChannelTransacted(transacted);
			container.afterPropertiesSet();
			assertEquals(2, ReflectionTestUtils.getField(container, "concurrentConsumers"));
			container.start();
			waitForNConsumers(container, 2);
			container.setConcurrentConsumers(1);
			waitForNConsumers(container, 1);
			container.setMaxConcurrentConsumers(3);
			RabbitTemplate template = new RabbitTemplate(singleConnectionFactory);
			for (int i = 0; i < 20; i++) {
				template.convertAndSend(QUEUE, "foo");
			}
			waitForNConsumers(container, 2);        // increased consumers due to work
			waitForNConsumers(container, 1, 20000); // should stop the extra consumer after 10 seconds idle
			container.setConcurrentConsumers(3);
			waitForNConsumers(container, 3);
			container.stop();
			waitForNConsumers(container, 0);
		}
		finally {
			container.stop();
			singleConnectionFactory.destroy();
		}
	}

	@Test
	public void testAddQueuesAndStartInCycle() throws Exception {
		final SingleConnectionFactory connectionFactory = new SingleConnectionFactory("localhost");
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener((MessageListener) message -> {
		});
		container.setConcurrentConsumers(2);
		container.afterPropertiesSet();

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		for (int i = 0; i < 20; i++) {
			Queue queue = new Queue("testAddQueuesAndStartInCycle" + i);
			admin.declareQueue(queue);
			container.addQueueNames(queue.getName());
			if (!container.isRunning()) {
				container.start();
			}
		}

		int n = 0;
		while (n++ < 100 && container.getActiveConsumerCount() != 2) {
			Thread.sleep(100);
		}
		assertEquals(2, container.getActiveConsumerCount());
		container.stop();
		for (int i = 0; i < 20; i++) {
			admin.deleteQueue("testAddQueuesAndStartInCycle" + i);
		}

		connectionFactory.destroy();
	}

	@Test
	public void testIncreaseMinAtMax() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setStartConsumerMinInterval(1);
		container.setConsecutiveActiveTrigger(1);
		container.setMessageListener((MessageListener) m -> {
			try {
				Thread.sleep(50);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		container.setQueueNames(QUEUE2);
		container.setConcurrentConsumers(2);
		container.setMaxConcurrentConsumers(5);
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = new RabbitTemplate(singleConnectionFactory);
		for (int i = 0; i < 20; i++) {
			template.convertAndSend(QUEUE2, "foo");
		}
		waitForNConsumers(container, 5);
		container.setConcurrentConsumers(4);
		Set<?> consumers = (Set<?>) TestUtils.getPropertyValue(container, "consumers");
		assertThat(consumers.size(), equalTo(5));

		container.stop();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testDecreaseMinAtMax() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setStartConsumerMinInterval(100);
		container.setConsecutiveActiveTrigger(1);
		container.setMessageListener((MessageListener) m -> {
			try {
				Thread.sleep(50);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		container.setQueueNames(QUEUE3);
		container.setConcurrentConsumers(2);
		container.setMaxConcurrentConsumers(3);
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = new RabbitTemplate(singleConnectionFactory);
		for (int i = 0; i < 20; i++) {
			template.convertAndSend(QUEUE3, "foo");
		}
		waitForNConsumers(container, 3);
		container.setConcurrentConsumers(1);
		Set<?> consumers = (Set<?>) TestUtils.getPropertyValue(container, "consumers");
		assertThat(consumers.size(), equalTo(3));

		container.stop();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testDecreaseMaxAtMax() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setStartConsumerMinInterval(100);
		container.setConsecutiveActiveTrigger(1);
		container.setMessageListener((MessageListener) m -> {
			try {
				Thread.sleep(50);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		container.setQueueNames(QUEUE4);
		container.setConcurrentConsumers(2);
		container.setMaxConcurrentConsumers(3);
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = new RabbitTemplate(singleConnectionFactory);
		for (int i = 0; i < 20; i++) {
			template.convertAndSend(QUEUE4, "foo");
		}
		waitForNConsumers(container, 3);
		container.setConcurrentConsumers(1);
		container.setMaxConcurrentConsumers(1);
		Set<?> consumers = (Set<?>) TestUtils.getPropertyValue(container, "consumers");
		assertThat(consumers.size(), equalTo(1));

		container.stop();
		singleConnectionFactory.destroy();
	}

	public void handleMessage(String foo) {
		logger.info(foo);
	}

	private void waitForNConsumers(SimpleMessageListenerContainer container, int n) throws InterruptedException {
		this.waitForNConsumers(container, n, 10000);
	}

	private void waitForNConsumers(SimpleMessageListenerContainer container, int n, int howLong)
			throws InterruptedException {

		int i = 0;
		while (true) {
			Set<?> consumers = (Set<?>) TestUtils.getPropertyValue(container, "consumers");
			if (n == 0 && consumers == null) {
				break;
			}
			if (consumers != null && consumers.size() == n) {
				break;
			}
			Thread.sleep(100);
			if (i++ > howLong / 100) {
				fail("Never reached " + n + " consumers");
			}
		}
	}

}
