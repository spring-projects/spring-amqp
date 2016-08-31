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
import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.LongRunningIntegrationTest;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Gary Russell
 *
 * @since 1.2.1
 *
 */
public class SimpleMessageListenerContainerLongTests {

	private final Log logger = LogFactory.getLog(SimpleMessageListenerContainerLongTests.class);

	@Rule
	public LongRunningIntegrationTest longTest = new LongRunningIntegrationTest();

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunningWithEmptyQueues("foo");

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
			container.setQueueNames("foo");
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
				template.convertAndSend("foo", "foo");
			}
			waitForNConsumers(container, 2);		// increased consumers due to work
			waitForNConsumers(container, 1, 20000); // should stop the extra consumer after 10 seconds idle
			container.setConcurrentConsumers(3);
			waitForNConsumers(container, 3);
			container.stop();
			waitForNConsumers(container, 0);
			singleConnectionFactory.destroy();
		}
		finally {
			container.stop();
		}
	}

	@Test
	public void testAddQueuesAndStartInCycle() throws Exception {
		final SingleConnectionFactory connectionFactory = new SingleConnectionFactory("localhost");
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener((MessageListener) message -> { });
		container.setConcurrentConsumers(2);
		container.afterPropertiesSet();

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		for (int i = 0; i < 20; i++) {
			AnonymousQueue anonymousQueue = new AnonymousQueue();
			admin.declareQueue(anonymousQueue);
			container.addQueueNames(anonymousQueue.getName());
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
			Map<?, ?> consumers = (Map<?, ?>) TestUtils.getPropertyValue(container, "consumers");
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
