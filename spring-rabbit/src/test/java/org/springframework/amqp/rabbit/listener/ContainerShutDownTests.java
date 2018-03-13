/*
 * Copyright 2017-2018 the original author or authors.
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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.utils.test.TestUtils;

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
		final CountDownLatch testEnded = new CountDownLatch(1);
		container.setMessageListener(m -> {
			try {
				latch.countDown();
				testEnded.await(30, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		final CountDownLatch startLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof AsyncConsumerStartedEvent) {
				startLatch.countDown();
			}
		});
		Connection connection = cf.createConnection();
		Map<?, ?> channels = TestUtils.getPropertyValue(connection, "target.delegate._channelManager._channelMap",
				Map.class);
		container.start();
		try {
			assertTrue(startLatch.await(30, TimeUnit.SECONDS));
			RabbitTemplate template = new RabbitTemplate(cf);
			template.convertAndSend("test.shutdown", "foo");
			assertTrue(latch.await(30, TimeUnit.SECONDS));
			assertThat(channels.size(), equalTo(2));
		}
		finally {
			container.stop();
			assertThat(channels.size(), equalTo(1));

			cf.destroy();
			testEnded.countDown();
		}
	}

}
