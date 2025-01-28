/*
 * Copyright 2016-2025 the original author or authors.
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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Address;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DirectReplyToMessageListenerContainer Tests.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@RabbitAvailable(queues = DirectReplyToMessageListenerContainerTests.TEST_RELEASE_CONSUMER_Q)
public class DirectReplyToMessageListenerContainerTests {

	public static final String TEST_RELEASE_CONSUMER_Q = "test.release.consumer";

	@Test
	public void testReleaseConsumerRace() throws Exception {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		DirectReplyToMessageListenerContainer container = new DirectReplyToMessageListenerContainer(connectionFactory);
		final CountDownLatch latch = new CountDownLatch(1);

		// Populate void MessageListener for wrapping in the DirectReplyToMessageListenerContainer
		container.setMessageListener(m -> {
		});

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
		GetResponse request = await()
				.pollDelay(Duration.ZERO)
				.until(() -> replyChannel.basicGet(TEST_RELEASE_CONSUMER_Q, true), req -> req != null);
		assertThat(request).isNotNull();
		replyChannel.basicPublish("", request.getProps().getReplyTo(), new BasicProperties(), "bar".getBytes());
		replyChannel.close();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

		ChannelHolder channel2 = container.getChannelHolder();
		assertThat(channel2.getChannel()).isSameAs(channel1.getChannel());
		container.releaseConsumerFor(channel1, false, null); // simulate race for future timeout/cancel and onMessage()
		Map<?, ?> inUse = TestUtils.getPropertyValue(container, "inUseConsumerChannels", Map.class);
		assertThat(inUse).hasSize(1);
		container.releaseConsumerFor(channel2, false, null);
		assertThat(inUse).hasSize(0);
		container.stop();
		connectionFactory.destroy();
	}

}
