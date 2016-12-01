/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * @author Gary Russell
 * @since 1.6
 *
 */
public class RabbitTemplatePublisherCallbacksIntegrationTests2 {

	private static final String ROUTE = "test.queue";

	private CachingConnectionFactory connectionFactoryWithConfirmsEnabled;

	private RabbitTemplate templateWithConfirmsEnabled;

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(ROUTE);

	@Before
	public void create() {
		connectionFactoryWithConfirmsEnabled = new CachingConnectionFactory();
		connectionFactoryWithConfirmsEnabled.setHost("localhost");
		// When using publisher confirms, the cache size needs to be large enough
		// otherwise channels can be closed before confirms are received.
		connectionFactoryWithConfirmsEnabled.setChannelCacheSize(100);
		connectionFactoryWithConfirmsEnabled.setPort(BrokerTestUtils.getPort());
		connectionFactoryWithConfirmsEnabled.setPublisherConfirms(true);
		templateWithConfirmsEnabled = new RabbitTemplate(connectionFactoryWithConfirmsEnabled);
	}

	@After
	public void cleanUp() {
		if (connectionFactoryWithConfirmsEnabled != null) {
			connectionFactoryWithConfirmsEnabled.destroy();
		}
		this.brokerIsRunning.removeTestQueues();
	}

	@Test
	public void test36Methods() throws Exception {
		this.templateWithConfirmsEnabled.convertAndSend(ROUTE, "foo");
		this.templateWithConfirmsEnabled.convertAndSend(ROUTE, "foo");
		assertMessageCountEquals(2L);
		assertEquals(Long.valueOf(1), this.templateWithConfirmsEnabled.execute(channel -> {
			final CountDownLatch latch = new CountDownLatch(2);
			String consumerTag = channel.basicConsume(ROUTE, new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					latch.countDown();
				}
			});
			long consumerCount = channel.consumerCount(ROUTE);
			assertTrue(latch.await(10, TimeUnit.SECONDS));
			channel.basicCancel(consumerTag);
			return consumerCount;
		}));
		assertMessageCountEquals(0L);
	}

	private void assertMessageCountEquals(long wanted) throws InterruptedException {
		long messageCount = determineMessageCount();
		int n = 0;
		while (messageCount < wanted && n++ < 100) {
			Thread.sleep(100);
			messageCount = determineMessageCount();
		}
		assertEquals(wanted, messageCount);
	}

	private Long determineMessageCount() {
		return this.templateWithConfirmsEnabled.execute(channel -> channel.messageCount(ROUTE));
	}

}
