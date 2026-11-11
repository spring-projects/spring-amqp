/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer.SimpleConsumer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.scheduling.TaskScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * Direct reply-to checkout and idle removal do not keep spare consumers.
 *
 * @author WooSeung Kim
 * @since 4.0.7
 */
class DirectReplyToMessageListenerContainerMockTests {

	private final AtomicReference<Runnable> monitor = new AtomicReference<>();

	@Test
	void failedConnectionCheckoutDoesNotLeaveASpareConsumer() throws Exception {
		AtomicBoolean failConnection = new AtomicBoolean(true);
		List<Channel> channels = new ArrayList<>();
		DirectReplyToMessageListenerContainer container = runningContainer(connectionFactory(failConnection, channels));
		try {
			assertThatExceptionOfType(AmqpConnectException.class)
					.isThrownBy(container::getChannelHolder);
			assertThat(consumers(container)).isEmpty();
			assertThat(channels).isEmpty();

			failConnection.set(false);
			ChannelHolder holder = container.getChannelHolder();
			assertThat(consumers(container)).hasSize(1);
			assertThat(channels).hasSize(1);

			container.releaseConsumerFor(holder, false, null);
			idle(container);
			assertThat(consumers(container)).isEmpty();
		}
		finally {
			container.stop();
		}
	}

	@Test
	void repeatedConnectionFailuresDoNotAccumulateConsumers() throws Exception {
		AtomicBoolean failConnection = new AtomicBoolean(true);
		List<Channel> channels = new ArrayList<>();
		DirectReplyToMessageListenerContainer container = runningContainer(connectionFactory(failConnection, channels));
		try {
			for (int i = 0; i < 3; i++) {
				assertThatExceptionOfType(AmqpConnectException.class)
						.isThrownBy(container::getChannelHolder);
			}

			failConnection.set(false);
			ChannelHolder holder = container.getChannelHolder();
			assertThat(consumers(container)).hasSize(1);
			assertThat(channels).hasSize(1);

			container.releaseConsumerFor(holder, false, null);
			idle(container);
			assertThat(consumers(container)).isEmpty();
		}
		finally {
			container.stop();
		}
	}

	@Test
	void consumerRestartedAfterAClosedChannelIsRemovedWhenIdle() throws Exception {
		List<Channel> channels = new ArrayList<>();
		DirectReplyToMessageListenerContainer container = runningContainer(connectionFactory(
				new AtomicBoolean(false), channels));
		try {
			container.releaseConsumerFor(container.getChannelHolder(), false, null);
			given(channels.get(0).isOpen()).willReturn(false);
			new DirectFieldAccessor(container).setPropertyValue("lastRestartAttempt", 0L);
			this.monitor.get().run();
			assertThat(consumers(container)).hasSize(1);
			assertThat(channels).hasSize(2);

			idle(container);
			assertThat(consumers(container)).isEmpty();
		}
		finally {
			container.stop();
		}
	}

	@Test
	void channelCacheTimeoutStillRetriesTheSameCheckout() throws Exception {
		List<Channel> channels = new ArrayList<>();
		AtomicInteger channelAttempts = new AtomicInteger();
		ConnectionFactory connectionFactory = mock();
		Connection connection = mock();
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(anyBoolean())).willAnswer(invocation -> {
			if (channelAttempts.getAndIncrement() == 0) {
				throw new AmqpTimeoutException("no channel in the cache");
			}
			return newOpenChannel(channels);
		});

		DirectReplyToMessageListenerContainer container = runningContainer(connectionFactory);
		try {
			ChannelHolder holder = container.getChannelHolder();
			assertThat(holder.getChannel()).isSameAs(channels.get(0));
			assertThat(consumers(container)).hasSize(1);
		}
		finally {
			container.stop();
		}
	}

	@Test
	void checkoutWhileOneConsumerIsInUseOpensAnother() throws Exception {
		List<Channel> channels = new ArrayList<>();
		DirectReplyToMessageListenerContainer container = runningContainer(connectionFactory(
				new AtomicBoolean(false), channels));
		try {
			ChannelHolder first = container.getChannelHolder();
			ChannelHolder second = container.getChannelHolder();
			assertThat(first.getChannel()).isNotSameAs(second.getChannel());
			assertThat(consumers(container)).hasSize(2);
			assertThat(channels).hasSize(2);

			container.releaseConsumerFor(first, false, null);
			container.releaseConsumerFor(second, false, null);
			idle(container);
			assertThat(consumers(container)).isEmpty();
		}
		finally {
			container.stop();
		}
	}

	@Test
	void templateRequestsFailingDuringABrokerOutageDoNotRaiseTheConsumerCount() throws Exception {
		AtomicBoolean failConnection = new AtomicBoolean(false);
		List<Channel> channels = new ArrayList<>();
		RabbitTemplate template = new RabbitTemplate(connectionFactory(failConnection, channels));
		template.setTaskExecutor(Runnable::run);
		template.setReplyTimeout(10);
		Message request = new Message("request".getBytes(), new MessageProperties());
		try {
			assertThat(template.sendAndReceive("exchange", "routing.key", request)).isNull();
			DirectReplyToMessageListenerContainer container = replyContainer(template);
			SimpleConsumer first = (SimpleConsumer) consumers(container).get(0);
			given(first.getChannel().isOpen()).willReturn(false);

			failConnection.set(true);
			for (int i = 0; i < 3; i++) {
				assertThatExceptionOfType(AmqpConnectException.class)
						.isThrownBy(() -> template.sendAndReceive("exchange", "routing.key", request));
			}
			assertThat(consumerCount(container)).isOne();

			failConnection.set(false);
			assertThat(template.sendAndReceive("exchange", "routing.key", request)).isNull();
		}
		finally {
			template.stop();
		}
	}

	private static ConnectionFactory connectionFactory(AtomicBoolean failConnection, List<Channel> channels) {
		ConnectionFactory connectionFactory = mock();
		Connection connection = mock();
		given(connectionFactory.createConnection()).willAnswer(invocation -> {
			if (failConnection.get()) {
				throw new IllegalStateException("broker down");
			}
			return connection;
		});
		given(connection.createChannel(anyBoolean())).willAnswer(invocation -> newOpenChannel(channels));
		return connectionFactory;
	}

	private DirectReplyToMessageListenerContainer runningContainer(ConnectionFactory connectionFactory) {
		TaskScheduler scheduler = mock();
		ScheduledFuture<?> monitorFuture = mock();
		willAnswer(invocation -> {
			this.monitor.set(invocation.getArgument(0));
			return monitorFuture;
		}).given(scheduler).scheduleAtFixedRate(any(Runnable.class), any(Duration.class));
		DirectReplyToMessageListenerContainer container = new DirectReplyToMessageListenerContainer(connectionFactory);
		container.setShutdownTimeout(0);
		container.setTaskScheduler(scheduler);
		container.setTaskExecutor(Runnable::run);
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	@SuppressWarnings("unchecked")
	private static void idle(DirectReplyToMessageListenerContainer container) {
		container.processMonitorTask();
		Map<Object, Long> whenUsed = (Map<Object, Long>) TestUtils.getPropertyValue(container, "whenUsed");
		whenUsed.replaceAll((consumer, usedAt) -> 0L);
		container.processMonitorTask();
	}

	private static DirectReplyToMessageListenerContainer replyContainer(RabbitTemplate template) {
		Map<?, ?> containers = (Map<?, ?>) TestUtils.getPropertyValue(template, "directReplyToContainers");
		return (DirectReplyToMessageListenerContainer) containers.values().iterator().next();
	}

	private static int consumerCount(DirectReplyToMessageListenerContainer container) {
		return ((AtomicInteger) TestUtils.getPropertyValue(container, "consumerCount")).get();
	}

	private static List<?> consumers(DirectReplyToMessageListenerContainer container) {
		return (List<?>) TestUtils.getPropertyValue(container, "consumers");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Channel newOpenChannel(List<Channel> channels) throws IOException {
		Channel channel = mock();
		AMQP.Queue.DeclareOk declareOk = mock();
		given(channel.isOpen()).willReturn(true);
		given(channel.queueDeclarePassive(anyString())).willReturn(declareOk);
		given(channel.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
				nullable(Map.class), any(Consumer.class))).willReturn("ctag-" + channels.size());
		channels.add(channel);
		return channel;
	}

}
