/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client.listener;

import java.time.Duration;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Sharang Gupta
 *
 * @since 4.2
 */
public class RabbitAmqpListenerContainerUnitTests {

	private static final long NO_TTL = 0xFFFF_FFFFL;

	private final ConsumerBuilder consumerBuilder = mock(ConsumerBuilder.class, RETURNS_SELF);

	private final AmqpConnectionFactory connectionFactory = mock();

	private final Consumer.Context context = mock();

	@Test
	void failedMessageIsDiscardedWhenDefaultRequeueIsFalse() {
		Consumer.MessageHandler handler = startContainer(false, (message) -> {
			throw new RuntimeException("listener failure");
		});

		handler.handle(this.context, amqpMessage());

		verify(this.context).discard();
		verify(this.context, never()).accept();
		verify(this.context, never()).requeue();
	}

	@Test
	void failedMessageIsRequeuedWhenDefaultRequeueIsTrue() {
		Consumer.MessageHandler handler = startContainer(true, (message) -> {
			throw new RuntimeException("listener failure");
		});

		handler.handle(this.context, amqpMessage());

		verify(this.context).requeue();
		verify(this.context, never()).accept();
		verify(this.context, never()).discard();
	}

	@Test
	void rejectAndDontRequeueIsDiscardedEvenWhenDefaultRequeueIsTrue() {
		Consumer.MessageHandler handler = startContainer(true, (message) -> {
			throw new AmqpRejectAndDontRequeueException("do not requeue");
		});

		handler.handle(this.context, amqpMessage());

		verify(this.context).discard();
		verify(this.context, never()).accept();
		verify(this.context, never()).requeue();
	}

	private Consumer.MessageHandler startContainer(boolean defaultRequeue, MessageListener listener) {
		Connection connection = mock();
		given(this.connectionFactory.getConnection()).willReturn(connection);
		given(connection.consumerBuilder()).willReturn(this.consumerBuilder);
		given(this.consumerBuilder.build()).willReturn(mock(Consumer.class));

		RabbitAmqpListenerContainer container = new RabbitAmqpListenerContainer(this.connectionFactory);
		container.setQueueNames("test.queue");
		container.setDefaultRequeue(defaultRequeue);
		container.setupMessageListener(listener);
		container.afterPropertiesSet();
		container.start();

		ArgumentCaptor<Consumer.MessageHandler> handlerCaptor = ArgumentCaptor.forClass(Consumer.MessageHandler.class);
		verify(this.consumerBuilder).messageHandler(handlerCaptor.capture());
		return handlerCaptor.getValue();
	}

	private static com.rabbitmq.client.amqp.Message amqpMessage() {
		com.rabbitmq.client.amqp.Message amqpMessage = mock();
		given(amqpMessage.body()).willReturn("test data".getBytes());
		given(amqpMessage.ttl()).willReturn(Duration.ofMillis(NO_TTL));
		return amqpMessage;
	}

}
