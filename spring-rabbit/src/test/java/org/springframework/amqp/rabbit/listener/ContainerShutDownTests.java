/*
 * Copyright 2017 the original author or authors.
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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.utils.test.TestUtils;

import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class ContainerShutDownTests {

	@ClassRule
	public static BrokerRunning brokerRunning = BrokerRunning.isRunningWithEmptyQueues("test.shutdown");

	@AfterClass
	public static void tearDown() {
		brokerRunning.removeTestQueues();
	}

	@Test
	public void testUninterruptibleListenerSMLC() throws Exception {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		testUninterruptibleListener(container);
	}

	@Test
	public void testUninterruptibleListenerDMLC() throws Exception {
		DirectMessageListenerContainer container = new DirectMessageListenerContainer();
		testUninterruptibleListener(container);
	}

	public void testUninterruptibleListener(AbstractMessageListenerContainer container) throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		container.setConnectionFactory(cf);
		container.setShutdownTimeout(500);
		container.setQueueNames("test.shutdown");
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicBoolean testEnded = new AtomicBoolean();
		container.setMessageListener(m -> {
			while (!testEnded.get()) {
				try {
					latch.countDown();
					Thread.sleep(100);
				}
				catch (InterruptedException e) {
					// Thread.currentThread().interrupt(); // eat it
				}
			}
		});
		final CountDownLatch startLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof AsyncConsumerStartedEvent) {
				startLatch.countDown();
			}
		});
		container.start();
		assertTrue(startLatch.await(10, TimeUnit.SECONDS));
		RabbitTemplate template = new RabbitTemplate(cf);
		template.execute(c -> {
			c.basicPublish("", "test.shutdown", new BasicProperties(), "foo".getBytes());
			RabbitUtils.setPhysicalCloseRequired(c, false);
			return null;
		});
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		Connection connection = cf.createConnection();
		Map<?, ?> channels = TestUtils.getPropertyValue(connection, "target.delegate._channelManager._channelMap", Map.class);
		assertThat(channels.size(), equalTo(2));
		container.stop();
		assertThat(channels.size(), equalTo(1));

		cf.destroy();
		testEnded.set(true);
	}

}
