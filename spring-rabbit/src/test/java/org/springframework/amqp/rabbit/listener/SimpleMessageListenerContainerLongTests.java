/*
 * Copyright 2002-2017 the original author or authors.
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
import static org.junit.Assert.fail;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.LongRunning;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.test.util.ReflectionTestUtils;

import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Gary Russell
 *
 * @since 1.2.1
 *
 */
@RabbitAvailable(queues = SimpleMessageListenerContainerLongTests.QUEUE)
@LongRunning
public class SimpleMessageListenerContainerLongTests {

	public static final String QUEUE = "SimpleMessageListenerContainerLongTests.queue";

	private final Log logger = LogFactory.getLog(SimpleMessageListenerContainerLongTests.class);

	private final SingleConnectionFactory connectionFactory;


	public SimpleMessageListenerContainerLongTests(ConnectionFactory connectionFactory) {
		this.connectionFactory = new SingleConnectionFactory(connectionFactory);
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
		SimpleMessageListenerContainer container =
				new SimpleMessageListenerContainer(this.connectionFactory);
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
			RabbitTemplate template = new RabbitTemplate(this.connectionFactory);
			for (int i = 0; i < 20; i++) {
				template.convertAndSend(QUEUE, "foo");
			}
			waitForNConsumers(container, 2);		// increased consumers due to work
			waitForNConsumers(container, 1, 20000); // should stop the extra consumer after 10 seconds idle
			container.setConcurrentConsumers(3);
			waitForNConsumers(container, 3);
			container.stop();
			waitForNConsumers(container, 0);
			this.connectionFactory.destroy();
		}
		finally {
			container.stop();
		}
	}

	@Test
	public void testAddQueuesAndStartInCycle() throws Exception {
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(
				this.connectionFactory);
		container.setMessageListener((MessageListener) message -> { });
		container.setConcurrentConsumers(2);
		container.afterPropertiesSet();

		RabbitAdmin admin = new RabbitAdmin(this.connectionFactory);
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


	public void handleMessage(String foo) {
		logger.info(foo);
	}

	private void waitForNConsumers(SimpleMessageListenerContainer container, int n) throws InterruptedException {
		this.waitForNConsumers(container, n, 10000);
	}

	private void waitForNConsumers(SimpleMessageListenerContainer container, int n, int howLong) throws InterruptedException {
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
