/*
 * Copyright 2017 the original author or authors.
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class DirectMessageListenerContainerMockTests {

	@Test
	public void testAlwaysCancelAutoRecoverConsumer() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		ChannelProxy channel = mock(ChannelProxy.class);
		Channel rabbitChannel = mock(AutorecoveringChannel.class);
		given(channel.getTargetChannel()).willReturn(rabbitChannel);

		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(anyBoolean())).willReturn(channel);
		given(channel.isOpen()).willReturn(true, false);
		given(channel.queueDeclarePassive(Mockito.anyString()))
				.willAnswer(invocation -> mock(AMQP.Queue.DeclareOk.class));
		given(channel.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
						anyMap(), any(Consumer.class))).willReturn("consumerTag");

		final CountDownLatch latch1 = new CountDownLatch(1);
		final AtomicInteger qos = new AtomicInteger();
		willAnswer(i -> {
			qos.set(i.getArgument(0));
			latch1.countDown();
			return null;
		}).given(channel).basicQos(anyInt());
		final CountDownLatch latch2 = new CountDownLatch(1);
		willAnswer(i -> {
			latch2.countDown();
			return null;
		}).given(channel).basicCancel("consumerTag");

		DirectMessageListenerContainer container = new DirectMessageListenerContainer(connectionFactory);
		container.setQueueNames("test");
		container.setPrefetchCount(2);
		container.setMonitorInterval(100);
		container.afterPropertiesSet();
		container.start();

		assertTrue(latch1.await(10, TimeUnit.SECONDS));
		assertThat(qos.get(), equalTo(2));
		assertTrue(latch2.await(10, TimeUnit.SECONDS));
		container.stop();
	}

}
