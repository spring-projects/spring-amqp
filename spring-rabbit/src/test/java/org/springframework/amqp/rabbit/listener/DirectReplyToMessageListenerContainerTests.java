/*
 * Copyright 2016-2018 the original author or authors.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.Address;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;

/**
 * DirectReplyToMessageListenerContainer Tests.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public class DirectReplyToMessageListenerContainerTests {

	private static final String TEST_RELEASE_CONSUMER_Q = "test.release.consumer";

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunningWithEmptyQueues(TEST_RELEASE_CONSUMER_Q);

	@After
	public void tearDown() {
		this.brokerRunning.removeTestQueues();
	}

	@Test
	public void testReleaseConsumerRace() throws Exception {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		DirectReplyToMessageListenerContainer container = new DirectReplyToMessageListenerContainer(connectionFactory);
		final CountDownLatch latch = new CountDownLatch(1);

		// Populate void MessageListener for wrapping in the DirectReplyToMessageListenerContainer
		container.setMessageListener(m -> { });

		// Extract actual ChannelAwareMessageListener from container
		// with the inUseConsumerChannels.remove(channel); operation
		final ChannelAwareMessageListener messageListener =
				TestUtils.getPropertyValue(container, "messageListener",
						ChannelAwareMessageListener.class);

		// Wrap actual listener for latch barrier exactly after inUseConsumerChannels.remove(channel);
		ChannelAwareMessageListener mockMessageListener =
				(message, channel) -> {
					try {
						messageListener.onMessage(message, channel);
					}
					finally {
						latch.countDown();
					}
				};

		// Populated mocked listener via reflection
		new DirectFieldAccessor(container)
				.setPropertyValue("messageListener", mockMessageListener);

		container.start();
		ChannelHolder channel1 = container.getChannelHolder();
		BasicProperties props = new BasicProperties().builder().replyTo(Address.AMQ_RABBITMQ_REPLY_TO).build();
		channel1.getChannel().basicPublish("", TEST_RELEASE_CONSUMER_Q, props, "foo".getBytes());
		Channel replyChannel = connectionFactory.createConnection().createChannel(false);
		GetResponse request = replyChannel.basicGet(TEST_RELEASE_CONSUMER_Q, true);
		int n = 0;
		while (n++ < 100 && request == null) {
			Thread.sleep(100);
			request = replyChannel.basicGet(TEST_RELEASE_CONSUMER_Q, true);
		}
		assertNotNull(request);
		replyChannel.basicPublish("", request.getProps().getReplyTo(), new BasicProperties(), "bar".getBytes());
		replyChannel.close();
		assertTrue(latch.await(10, TimeUnit.SECONDS));

		ChannelHolder channel2 = container.getChannelHolder();
		assertSame(channel1.getChannel(), channel2.getChannel());
		container.releaseConsumerFor(channel1, false, null); // simulate race for future timeout/cancel and onMessage()
		Map<?, ?> inUse = TestUtils.getPropertyValue(container, "inUseConsumerChannels", Map.class);
		assertThat(inUse.size(), equalTo(1));
		container.releaseConsumerFor(channel2, false, null);
		assertThat(inUse.size(), equalTo(0));
		container.stop();
		connectionFactory.destroy();
	}

}
