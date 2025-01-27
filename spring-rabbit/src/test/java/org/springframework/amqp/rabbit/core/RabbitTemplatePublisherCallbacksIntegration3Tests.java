/*
 * Copyright 2018-2025 the original author or authors.
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
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;

import com.rabbitmq.client.Channel;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1
 *
 */
@RabbitAvailable(queues = {RabbitTemplatePublisherCallbacksIntegration3Tests.QUEUE1,
		RabbitTemplatePublisherCallbacksIntegration3Tests.QUEUE2,
		RabbitTemplatePublisherCallbacksIntegration3Tests.QUEUE3})
public class RabbitTemplatePublisherCallbacksIntegration3Tests {

	public static final String QUEUE1 = "synthetic.nack";

	public static final String QUEUE2 = "defer.close";

	public static final String QUEUE3 = "confirm.send.receive";

	@Test
	public void testRepublishOnNackThreadNoExchange() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		cf.setPublisherConfirmType(ConfirmType.CORRELATED);
		final RabbitTemplate template = new RabbitTemplate(cf);
		final CountDownLatch confirmLatch = new CountDownLatch(2);
		template.setConfirmCallback((cd, a, c) -> {
			confirmLatch.countDown();
			if (confirmLatch.getCount() == 1) {
				template.convertAndSend(QUEUE1, cd.getId());
			}
		});
		template.convertAndSend("bad.exchange", "junk", "foo", new CorrelationData("foo"));
		assertThat(confirmLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(template.receive(QUEUE1, 10_000)).isNotNull();
	}

	@Test
	public void testDeferredChannelCacheNack() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		cf.setPublisherReturns(true);
		cf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ApplicationContext mockApplicationContext = mock();
		cf.setApplicationContext(mockApplicationContext);
		RabbitTemplate template = new RabbitTemplate(cf);
		CountDownLatch returnLatch = new CountDownLatch(1);
		CountDownLatch confirmLatch = new CountDownLatch(1);
		AtomicInteger cacheCount = new AtomicInteger();
		AtomicBoolean returnCalledFirst = new AtomicBoolean();
		template.setConfirmCallback((cd, a, c) -> {
			cacheCount.set(TestUtils.getPropertyValue(cf, "cachedChannelsNonTransactional", List.class).size());
			returnCalledFirst.set(returnLatch.getCount() == 0);
			confirmLatch.countDown();
		});
		template.setReturnsCallback((returned) -> {
			returnLatch.countDown();
		});
		template.setMandatory(true);
		Connection conn = cf.createConnection();
		Channel channel1 = conn.createChannel(false);
		Channel channel2 = conn.createChannel(false);
		channel1.close();
		channel2.close();
		conn.close();
		assertThat(TestUtils.getPropertyValue(cf, "cachedChannelsNonTransactional", List.class).size()).isEqualTo(2);
		CorrelationData correlationData = new CorrelationData("foo");
		template.convertAndSend("", QUEUE2 + "junk", "foo", correlationData);
		assertThat(returnLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(confirmLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(cacheCount.get()).isEqualTo(1);
		assertThat(returnCalledFirst.get()).isTrue();
		assertThat(correlationData.getReturned()).isNotNull();
		cf.onApplicationEvent(new ContextClosedEvent(mockApplicationContext));
		cf.destroy();
	}

	@Test
	public void testDeferredChannelCacheAck() throws Exception {
		CachingConnectionFactory cf =
				new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		cf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ApplicationContext mockApplicationContext = mock();
		cf.setApplicationContext(mockApplicationContext);
		final RabbitTemplate template = new RabbitTemplate(cf);
		final CountDownLatch confirmLatch = new CountDownLatch(1);
		final AtomicInteger cacheCount = new AtomicInteger();
		template.setConfirmCallback((cd, a, c) -> {
			cacheCount.set(TestUtils.getPropertyValue(cf, "cachedChannelsNonTransactional", List.class).size());
			confirmLatch.countDown();
		});
		template.setMandatory(true);
		Connection conn = cf.createConnection();
		Channel channel1 = conn.createChannel(false);
		Channel channel2 = conn.createChannel(false);
		channel1.close();
		channel2.close();
		conn.close();
		assertThat(TestUtils.getPropertyValue(cf, "cachedChannelsNonTransactional", List.class).size()).isEqualTo(2);
		template.convertAndSend("", QUEUE2, "foo", new CorrelationData("foo"));
		assertThat(confirmLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(cacheCount.get()).isEqualTo(1);
		cf.onApplicationEvent(new ContextClosedEvent(mockApplicationContext));
		cf.destroy();
	}

	@Test
	public void testTwoSendsAndReceivesDRTMLC() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		cf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ApplicationContext mockApplicationContext = mock();
		cf.setApplicationContext(mockApplicationContext);
		RabbitTemplate template = new RabbitTemplate(cf);
		template.setReplyTimeout(0);
		final CountDownLatch confirmLatch = new CountDownLatch(2);
		template.setConfirmCallback((cd, a, c) -> {
			confirmLatch.countDown();
		});
		template.convertSendAndReceive("", QUEUE3, "foo", new CorrelationData("foo"));
		template.convertSendAndReceive("", QUEUE3, "foo", new CorrelationData("foo")); // listener not registered
		assertThat(confirmLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(template.receive(QUEUE3, 10_000)).isNotNull();
		assertThat(template.receive(QUEUE3, 10_000)).isNotNull();
		cf.onApplicationEvent(new ContextClosedEvent(mockApplicationContext));
		cf.destroy();
	}

}
