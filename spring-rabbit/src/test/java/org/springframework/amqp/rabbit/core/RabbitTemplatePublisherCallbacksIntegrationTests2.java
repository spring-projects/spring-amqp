/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
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

	public static final String ROUTE2 = "test.queue.RabbitTemplatePublisherCallbacksIntegrationTests2.route";

	private CachingConnectionFactory connectionFactoryWithConfirmsEnabled;

	private RabbitTemplate templateWithConfirmsEnabled;

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(ROUTE);

	@Before
	public void create() {
		connectionFactoryWithConfirmsEnabled = new CachingConnectionFactory();
		connectionFactoryWithConfirmsEnabled.setHost("localhost");
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

	@Test
	public void routingWithConfirmsNoListener() throws Exception {
		routingWithConfirms(false);
	}

	@Test
	public void routingWithConfirmsListener() throws Exception {
		routingWithConfirms(true);
	}

	private void routingWithConfirms(boolean listener) throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		SimpleRoutingConnectionFactory rcf = new SimpleRoutingConnectionFactory();
		rcf.setDefaultTargetConnectionFactory(this.connectionFactoryWithConfirmsEnabled);
		this.templateWithConfirmsEnabled.setConnectionFactory(rcf);
		if (listener) {
			this.templateWithConfirmsEnabled.setConfirmCallback((correlationData, ack, cause) -> {
				latch.countDown();
			});
		}
		this.templateWithConfirmsEnabled.setMandatory(true);
		CorrelationData corr = new CorrelationData("foo");
		this.templateWithConfirmsEnabled.convertAndSend("", ROUTE2, "foo", corr);
		assertTrue(corr.getFuture().get(10, TimeUnit.SECONDS).isAck());
		if (listener) {
			assertTrue(latch.await(10, TimeUnit.SECONDS));
		}
		corr = new CorrelationData("bar");
		this.templateWithConfirmsEnabled.convertAndSend("", "bad route", "foo", corr);
		assertTrue(corr.getFuture().get(10, TimeUnit.SECONDS).isAck());
		assertNotNull(corr.getReturnedMessage());
	}

	@Test
	public void routingWithSimpleConfirms() throws Exception {
		SimpleRoutingConnectionFactory rcf = new SimpleRoutingConnectionFactory();
		rcf.setDefaultTargetConnectionFactory(this.connectionFactoryWithConfirmsEnabled);
		this.templateWithConfirmsEnabled.setConnectionFactory(rcf);
		assertTrue(this.templateWithConfirmsEnabled.<Boolean>invoke(template -> {
				template.convertAndSend("", ROUTE2, "foo");
				template.waitForConfirmsOrDie(10_000);
				return true;
		}));
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
