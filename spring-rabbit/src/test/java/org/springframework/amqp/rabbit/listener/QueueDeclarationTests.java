/*
 * Copyright 2023-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.ApplicationContext;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;

/**
 * @author Gary Russell
 * @since 2.4.12
 *
 */
public class QueueDeclarationTests {

	@Test
	void redeclareWhenQueue() throws IOException, InterruptedException {
		AmqpAdmin admin = mock(AmqpAdmin.class);
		ApplicationContext context = mock(ApplicationContext.class);
		final CountDownLatch latch = new CountDownLatch(1);
		SimpleMessageListenerContainer container = createContainer(admin, latch);
		given(context.getBeansOfType(Queue.class, false, false)).willReturn(Map.of("foo", new Queue("test")));
		given(context.getBeansOfType(Declarables.class, false, false)).willReturn(new HashMap<>());
		container.setApplicationContext(context);
		container.start();

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(admin).initialize();
		container.stop();
	}

	@Test
	void redeclareWhenDeclarables() throws IOException, InterruptedException {
		AmqpAdmin admin = mock(AmqpAdmin.class);
		ApplicationContext context = mock(ApplicationContext.class);
		final CountDownLatch latch = new CountDownLatch(1);
		SimpleMessageListenerContainer container = createContainer(admin, latch);
		given(context.getBeansOfType(Queue.class, false, false)).willReturn(new HashMap<>());
		given(context.getBeansOfType(Declarables.class, false, false))
				.willReturn(Map.of("foo", new Declarables(List.of(new Queue("test")))));
		container.setApplicationContext(context);
		container.start();

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(admin).initialize();
		container.stop();
	}

	private SimpleMessageListenerContainer createContainer(AmqpAdmin admin, final CountDownLatch latch)
			throws IOException {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		ChannelProxy channel = mock(ChannelProxy.class);
		Channel rabbitChannel = mock(AutorecoveringChannel.class);
		given(channel.getTargetChannel()).willReturn(rabbitChannel);

		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(anyBoolean())).willReturn(channel);
		final AtomicBoolean isOpen = new AtomicBoolean(true);
		willAnswer(i -> isOpen.get()).given(channel).isOpen();
		given(channel.queueDeclarePassive(Mockito.anyString()))
				.willAnswer(invocation -> mock(AMQP.Queue.DeclareOk.class));
		given(channel.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
						anyMap(), any(Consumer.class))).willReturn("consumerTag");

		willAnswer(i -> {
			latch.countDown();
			return null;
		}).given(channel).basicQos(anyInt(), anyBoolean());

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("test");
		container.setPrefetchCount(2);
		container.setReceiveTimeout(10);
		container.setAmqpAdmin(admin);
		container.afterPropertiesSet();
		return container;
	}

}
