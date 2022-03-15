/*
 * Copyright 2017-2021 the original author or authors.
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
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
		final AtomicBoolean isOpen = new AtomicBoolean(true);
		willAnswer(i -> isOpen.get()).given(channel).isOpen();
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
		}).given(channel).basicQos(anyInt(), anyBoolean());
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

		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(qos.get()).isEqualTo(2);
		isOpen.set(false);
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	@Test
	public void testDeferredAcks() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		ChannelProxy channel = mock(ChannelProxy.class);
		Channel rabbitChannel = mock(AutorecoveringChannel.class);
		given(channel.getTargetChannel()).willReturn(rabbitChannel);

		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(anyBoolean())).willReturn(channel);
		given(channel.isOpen()).willReturn(true);
		given(channel.queueDeclarePassive(Mockito.anyString()))
				.willAnswer(invocation -> mock(AMQP.Queue.DeclareOk.class));
		final AtomicReference<Consumer> consumer = new AtomicReference<>();
		final CountDownLatch latch1 = new CountDownLatch(1);
		willAnswer(i -> {
			consumer.set(i.getArgument(6));
			consumer.get().handleConsumeOk("consumerTag");
			latch1.countDown();
			return "consumerTag";
		}).given(channel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
						anyMap(), any(Consumer.class));

		final AtomicInteger qos = new AtomicInteger();
		willAnswer(i -> {
			qos.set(i.getArgument(0));
			return null;
		}).given(channel).basicQos(anyInt(), anyBoolean());
		final CountDownLatch latch2 = new CountDownLatch(2);
		final CountDownLatch latch3 = new CountDownLatch(1);
		willAnswer(i -> {
			if (i.getArgument(0).equals(10L) || i.getArgument(0).equals(16L)) {
				latch2.countDown();
			}
			else if (i.getArgument(0).equals(17L)) {
				latch3.countDown();
			}
			return null;
		}).given(channel).basicAck(anyLong(), anyBoolean());
		final CountDownLatch latch4 = new CountDownLatch(1);
		willAnswer(i -> {
			latch4.countDown();
			return null;
		}).given(channel).basicNack(19L, true, true);

		DirectMessageListenerContainer container = new DirectMessageListenerContainer(connectionFactory);
		container.setQueueNames("test");
		container.setPrefetchCount(2);
		container.setMonitorInterval(100);
		container.setMessagesPerAck(10);
		container.setAckTimeout(100);
		container.setMessageListener(m -> {
			if (m.getMessageProperties().getDeliveryTag() == 19L) {
				throw new RuntimeException("TestNackAndPendingAcks");
			}
		});
		container.afterPropertiesSet();
		container.start();

		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(qos.get()).isEqualTo(10);
		BasicProperties props = new BasicProperties();
		byte[] body = new byte[1];
		for (long i = 1; i < 16; i++) {
			consumer.get().handleDelivery("consumerTag", envelope(i), props, body);
		}
		Thread.sleep(200);
		consumer.get().handleDelivery("consumerTag", envelope(16), props, body);
		// should get 2 acks #10 and #16 (timeout)
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		consumer.get().handleDelivery("consumerTag", envelope(17), props, body);
		verify(channel).basicAck(10L, true);
		verify(channel).basicAck(16L, true);
		assertThat(latch3.await(10, TimeUnit.SECONDS)).isTrue();
		// monitor task timeout
		verify(channel).basicAck(17L, true);
		consumer.get().handleDelivery("consumerTag", envelope(18), props, body);
		consumer.get().handleDelivery("consumerTag", envelope(19), props, body);
		assertThat(latch4.await(10, TimeUnit.SECONDS)).isTrue();
		// pending acks before nack
		verify(channel).basicAck(18L, true);
		verify(channel).basicNack(19L, true, true);
		consumer.get().handleDelivery("consumerTag", envelope(20), props, body);
		final CountDownLatch latch5 = new CountDownLatch(1);
		willAnswer(i -> {
			consumer.get().handleCancelOk("consumerTag");
			latch5.countDown();
			return null;
		}).given(channel).basicCancel("consumerTag");
		Executors.newSingleThreadExecutor().execute(container::stop);
		assertThat(latch5.await(10, TimeUnit.SECONDS)).isTrue();
		// pending acks on stop
		verify(channel).basicAck(20L, true);
	}

	@Test
	public void testRemoveQueuesWhileNotConnected() throws Exception {
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

		final CountDownLatch latch1 = new CountDownLatch(2);
		final CountDownLatch latch3 = new CountDownLatch(3);

		willAnswer(i -> {
			latch3.countDown();
			return "consumerTag";
		}).given(channel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
						anyMap(), any(Consumer.class));

		final AtomicInteger qos = new AtomicInteger();
		willAnswer(i -> {
			qos.set(i.getArgument(0));
			latch1.countDown();
			return null;
		}).given(channel).basicQos(anyInt(), anyBoolean());
		final CountDownLatch latch2 = new CountDownLatch(2);
		willAnswer(i -> {
			latch2.countDown();
			return null;
		}).given(channel).basicCancel("consumerTag");

		DirectMessageListenerContainer container = new DirectMessageListenerContainer(connectionFactory);
		container.setQueueNames("test1", "test2");
		container.setPrefetchCount(2);
		container.setMonitorInterval(100);
		container.setFailedDeclarationRetryInterval(100);
		container.setRecoveryInterval(100);
		container.setShutdownTimeout(1);
		container.afterPropertiesSet();
		container.start();

		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(qos.get()).isEqualTo(2);
		isOpen.set(false);
		container.removeQueueNames("test1");
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		isOpen.set(true);
		assertThat(latch3.await(10, TimeUnit.SECONDS)).isTrue();

		verify(channel, times(1)).basicConsume(eq("test1"), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
				anyMap(), any(Consumer.class));
		verify(channel, times(2)).basicConsume(eq("test2"), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
				anyMap(), any(Consumer.class));

		container.stop();
	}

	@Test
	public void testMonitorCancelsAfterBadAckEvenIfChannelReportsOpen() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		ChannelProxy channel = mock(ChannelProxy.class);
		Channel rabbitChannel = mock(Channel.class);
		given(channel.getTargetChannel()).willReturn(rabbitChannel);

		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(anyBoolean())).willReturn(channel);
		given(channel.isOpen()).willReturn(true);
		given(channel.queueDeclarePassive(Mockito.anyString()))
				.willAnswer(invocation -> mock(AMQP.Queue.DeclareOk.class));
		AtomicReference<Consumer> consumer = new AtomicReference<>();
		final CountDownLatch latch1 = new CountDownLatch(1);
		final CountDownLatch latch2 = new CountDownLatch(1);
		willAnswer(inv -> {
			consumer.set(inv.getArgument(6));
			latch1.countDown();
			return "consumerTag";
		}).given(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
						anyMap(), any(Consumer.class));

		willThrow(new RuntimeException("bad ack")).given(channel).basicAck(1L, false);
		willAnswer(inv -> {
			consumer.get().handleCancelOk("consumerTag");
			latch2.countDown();
			return null;
		}).given(channel).basicCancel("consumerTag");

		DirectMessageListenerContainer container = new DirectMessageListenerContainer(connectionFactory);
		container.setQueueNames("test");
		container.setPrefetchCount(2);
		container.setMonitorInterval(100);
		container.setMessageListener(mock(MessageListener.class));
		container.afterPropertiesSet();
		container.start();

		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		consumer.get().handleDelivery("consumerTag", envelope(1L), new BasicProperties(), new byte[1]);
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	@Test
	public void testMonitorCancelsAfterTargetChannelChanges() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		ChannelProxy channel = mock(ChannelProxy.class);
		Channel rabbitChannel1 = mock(Channel.class);
		Channel rabbitChannel2 = mock(Channel.class);
		AtomicReference<Channel> target = new AtomicReference<>(rabbitChannel1);
		willAnswer(inv -> {
			return target.get();
		}).given(channel).getTargetChannel();

		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(anyBoolean())).willReturn(channel);
		given(channel.isOpen()).willReturn(true);
		given(channel.queueDeclarePassive(Mockito.anyString()))
				.willAnswer(invocation -> mock(AMQP.Queue.DeclareOk.class));
		AtomicReference<Consumer> consumer = new AtomicReference<>();
		final CountDownLatch latch1 = new CountDownLatch(1);
		final CountDownLatch latch2 = new CountDownLatch(1);
		willAnswer(inv -> {
			consumer.set(inv.getArgument(6));
			latch1.countDown();
			return "consumerTag";
		}).given(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
						anyMap(), any(Consumer.class));

		willAnswer(inv -> {
			consumer.get().handleCancelOk("consumerTag");
			latch2.countDown();
			return null;
		}).given(channel).basicCancel("consumerTag");

		DirectMessageListenerContainer container = new DirectMessageListenerContainer(connectionFactory);
		container.setQueueNames("test");
		container.setPrefetchCount(2);
		container.setMonitorInterval(100);
		container.setMessageListener(msg -> {
			target.set(rabbitChannel2);
		});
		container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
		container.afterPropertiesSet();
		container.start();

		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		consumer.get().handleDelivery("consumerTag", envelope(1L), new BasicProperties(), new byte[1]);
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	@Test
	void monitorTaskThreadName() {
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(mock(ConnectionFactory.class));
		assertThat(container.getListenerId()).isEqualTo("not.a.Spring.bean");
		container.setBeanName("aBean");
		assertThat(container.getListenerId()).isEqualTo("aBean");
		container.setListenerId("id");
		assertThat(container.getListenerId()).isEqualTo("id");
		container.afterPropertiesSet();
		assertThat(container).extracting("taskScheduler")
				.extracting("threadNamePrefix")
				.asString()
				.startsWith("id-consumerMonitor");

		container = new DirectMessageListenerContainer(mock(ConnectionFactory.class));
		container.setBeanName("aBean");
		container.afterPropertiesSet();
		assertThat(container).extracting("taskScheduler")
				.extracting("threadNamePrefix")
				.asString()
				.startsWith("aBean-consumerMonitor");
	}

	private Envelope envelope(long tag) {
		return new Envelope(tag, false, "", "");
	}

}
