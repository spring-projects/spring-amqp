/*
 * Copyright 2016-2022 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * @author Gary Russell
 * @since 1.6
 *
 */
@RabbitAvailable(queues = { RabbitTemplatePublisherCallbacksIntegrationTests2.ROUTE,
		RabbitTemplatePublisherCallbacksIntegrationTests2.ROUTE2 })
public class RabbitTemplatePublisherCallbacksIntegrationTests2 {

	public static final String ROUTE = "test.queue.RabbitTemplatePublisherCallbacksIntegrationTests2";

	public static final String ROUTE2 = "test.queue.RabbitTemplatePublisherCallbacksIntegrationTests2.route";

	private CachingConnectionFactory connectionFactoryWithConfirmsEnabled;

	private RabbitTemplate templateWithConfirmsEnabled;

	@BeforeEach
	void create() {
		connectionFactoryWithConfirmsEnabled = new CachingConnectionFactory();
		connectionFactoryWithConfirmsEnabled.setHost("localhost");
		connectionFactoryWithConfirmsEnabled.setChannelCacheSize(100);
		connectionFactoryWithConfirmsEnabled.setPort(BrokerTestUtils.getPort());
		connectionFactoryWithConfirmsEnabled.setPublisherConfirmType(ConfirmType.CORRELATED);
		connectionFactoryWithConfirmsEnabled.setPublisherReturns(true);
		templateWithConfirmsEnabled = new RabbitTemplate(connectionFactoryWithConfirmsEnabled);
	}

	@AfterEach
	void cleanUp() {
		if (connectionFactoryWithConfirmsEnabled != null) {
			connectionFactoryWithConfirmsEnabled.destroy();
		}
	}

	@Test
	void test36Methods() throws Exception {
		this.templateWithConfirmsEnabled.convertAndSend(ROUTE, "foo");
		this.templateWithConfirmsEnabled.convertAndSend(ROUTE, "foo");
		assertMessageCountEquals(2L);
		Long result = this.templateWithConfirmsEnabled.execute(channel -> {
			final CountDownLatch latch = new CountDownLatch(2);
			String consumerTag = channel.basicConsume(ROUTE, new DefaultConsumer(channel) {

				@Override
				public void handleDelivery(String ag, Envelope envelope, BasicProperties properties, byte[] body) {
					latch.countDown();
				}

			});
			long consumerCount = channel.consumerCount(ROUTE);
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			channel.basicCancel(consumerTag);
			return consumerCount;
		});
		assertThat(result).isEqualTo(1L);
		assertMessageCountEquals(0L);
	}

	@Test
	void routingWithConfirmsNoListener() throws Exception {
		routingWithConfirms(false);
	}

	@Test
	void routingWithConfirmsListener() throws Exception {
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
		CorrelationData corr = new CorrelationData();
		this.templateWithConfirmsEnabled.convertAndSend("", ROUTE2, "foo", corr);
		assertThat(corr.getCompletableFuture().get(10, TimeUnit.SECONDS).isAck()).isTrue();
		if (listener) {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		corr = new CorrelationData();
		this.templateWithConfirmsEnabled.convertAndSend("", "bad route", "foo", corr);
		assertThat(corr.getCompletableFuture().get(10, TimeUnit.SECONDS).isAck()).isTrue();
		assertThat(corr.getReturned()).isNotNull();
	}

	@Test
	void routingWithSimpleConfirms() throws Exception {
		SimpleRoutingConnectionFactory rcf = new SimpleRoutingConnectionFactory();
		rcf.setDefaultTargetConnectionFactory(this.connectionFactoryWithConfirmsEnabled);
		this.templateWithConfirmsEnabled.setConnectionFactory(rcf);
		assertThat(this.templateWithConfirmsEnabled.<Boolean>invoke(template -> {
				template.convertAndSend("", ROUTE2, "foo");
				template.waitForConfirmsOrDie(10_000);
				return true;
		})).isTrue();
	}

	private void assertMessageCountEquals(long wanted) throws InterruptedException {
		long messageCount = determineMessageCount();
		int n = 0;
		while (messageCount < wanted && n++ < 100) {
			Thread.sleep(100);
			messageCount = determineMessageCount();
		}
		assertThat(messageCount).isEqualTo(wanted);
	}

	private Long determineMessageCount() {
		return this.templateWithConfirmsEnabled.execute(channel -> channel.messageCount(ROUTE));
	}

}
