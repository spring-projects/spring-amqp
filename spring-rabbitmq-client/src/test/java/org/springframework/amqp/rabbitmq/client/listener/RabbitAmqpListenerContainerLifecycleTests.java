/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client.listener;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Sharang Gupta
 *
 * @since 4.2
 */
public class RabbitAmqpListenerContainerLifecycleTests {

	private final ConsumerBuilder consumerBuilder = mock(ConsumerBuilder.class, RETURNS_SELF);

	private final AmqpConnectionFactory connectionFactory = mock();

	private final RabbitAmqpListenerContainer container = new RabbitAmqpListenerContainer(this.connectionFactory);

	@BeforeEach
	void setUp() {
		Connection connection = mock();
		given(this.connectionFactory.getConnection()).willReturn(connection);
		given(connection.consumerBuilder()).willReturn(this.consumerBuilder);
		this.container.setQueueNames("q1", "q2");
		this.container.setupMessageListener(mock(MessageListener.class));
		this.container.afterPropertiesSet();
	}

	@Test
	void failedStartClosesBuiltConsumersAndLeavesContainerStopped() {
		Consumer firstConsumer = mock();
		given(this.consumerBuilder.build())
				.willReturn(firstConsumer)
				.willThrow(new AmqpException("The queue 'q2' does not exist"));

		assertThatExceptionOfType(AmqpException.class).isThrownBy(this.container::start);

		assertThat(this.container.isRunning()).isFalse();
		verify(firstConsumer).close();
	}

	@Test
	void startCanBeRetriedAfterFailure() {
		given(this.consumerBuilder.build())
				.willReturn(mock(Consumer.class))
				.willThrow(new AmqpException("The queue 'q2' does not exist"))
				.willReturn(mock(Consumer.class));

		assertThatExceptionOfType(AmqpException.class).isThrownBy(this.container::start);
		this.container.start();

		assertThat(this.container.isRunning()).isTrue();
		verify(this.consumerBuilder, times(4)).build();
	}

}
