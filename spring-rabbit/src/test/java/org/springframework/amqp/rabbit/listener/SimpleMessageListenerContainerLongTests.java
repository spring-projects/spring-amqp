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
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

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
@RabbitAvailable(queues = {
		SimpleMessageListenerContainerLongTests.QUEUE,
		SimpleMessageListenerContainerLongTests.QUEUE2,
		SimpleMessageListenerContainerLongTests.QUEUE3,
		SimpleMessageListenerContainerLongTests.QUEUE4
})
@LongRunning
public class SimpleMessageListenerContainerLongTests {

	public static final String QUEUE = "SimpleMessageListenerContainerLongTests.queue";

	public static final String QUEUE2 = "SimpleMessageListenerContainerLongTests.queue2";

	public static final String QUEUE3 = "SimpleMessageListenerContainerLongTests.queue3";

	public static final String QUEUE4 = "SimpleMessageListenerContainerLongTests.queue4";

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
			container.setReceiveTimeout(10);
			container.afterPropertiesSet();
			assertThat(ReflectionTestUtils.getField(container, "concurrentConsumers")).isEqualTo(2);
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
	public void testAddQueuesAndStartInCycle() {
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(
				this.connectionFactory);
		container.setMessageListener(message -> { });
		container.setConcurrentConsumers(2);
		container.setReceiveTimeout(10);
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

		await().until(() -> container.getActiveConsumerCount() == 2);
		container.stop();
		for (int i = 0; i < 20; i++) {
			admin.deleteQueue("testAddQueuesAndStartInCycle" + i);
		}
		connectionFactory.destroy();
	}

	@Test
	public void testIncreaseMinAtMax() throws Exception {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactory);
		container.setStartConsumerMinInterval(100);
		container.setConsecutiveActiveTrigger(1);
		container.setReceiveTimeout(10);
		container.setMessageListener(m -> {
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
		RabbitTemplate template = new RabbitTemplate(this.connectionFactory);
		for (int i = 0; i < 20; i++) {
			template.convertAndSend(QUEUE2, "foo");
		}
		waitForNConsumers(container, 5);
		container.setConcurrentConsumers(4);
		Set<?> consumers = (Set<?>) TestUtils.getPropertyValue(container, "consumers");
		assertThat(consumers).hasSize(5);
	}

	@Test
	public void testDecreaseMinAtMax() throws Exception {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactory);
		container.setStartConsumerMinInterval(100);
		container.setConsecutiveActiveTrigger(1);
		container.setMessageListener(m -> {
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
		container.setReceiveTimeout(10);
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = new RabbitTemplate(this.connectionFactory);
		for (int i = 0; i < 20; i++) {
			template.convertAndSend(QUEUE3, "foo");
		}
		waitForNConsumers(container, 3);
		container.setConcurrentConsumers(1);
		Set<?> consumers = (Set<?>) TestUtils.getPropertyValue(container, "consumers");
		assertThat(consumers).hasSize(3);
	}

	@Test
	public void testDecreaseMaxAtMax() throws Exception {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactory);
		container.setStartConsumerMinInterval(100);
		container.setConsecutiveActiveTrigger(1);
		container.setMessageListener(m -> {
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
		container.setReceiveTimeout(10);
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = new RabbitTemplate(this.connectionFactory);
		for (int i = 0; i < 20; i++) {
			template.convertAndSend(QUEUE4, "foo");
		}
		waitForNConsumers(container, 3);
		container.setConcurrentConsumers(1);
		container.setMaxConcurrentConsumers(1);
		Set<?> consumers = (Set<?>) TestUtils.getPropertyValue(container, "consumers");
		assertThat(consumers).hasSize(1);
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
