/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client.listener;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;
import org.springframework.scheduling.TaskScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Sharang Gupta
 *
 * @since 4.2
 */
public class RabbitAmqpListenerContainerPostProcessorTests {

	private static final long NO_TTL = 0xFFFF_FFFFL;

	private final ConsumerBuilder consumerBuilder = mock(ConsumerBuilder.class, RETURNS_SELF);

	private final AmqpConnectionFactory connectionFactory = mock();

	private final MessageListener listener = mock();

	private final RabbitAmqpListenerContainer container = new RabbitAmqpListenerContainer(this.connectionFactory);

	@BeforeEach
	void setUp() {
		Connection connection = mock();
		given(this.connectionFactory.getConnection()).willReturn(connection);
		given(connection.consumerBuilder()).willReturn(this.consumerBuilder);
		given(this.consumerBuilder.build()).willReturn(mock(Consumer.class));
		this.container.setQueueNames("test.queue");
		this.container.setupMessageListener(this.listener);
	}

	@Test
	void afterReceivePostProcessorsApplyToPlainMessageListener() {
		this.container.setAfterReceivePostProcessors(new DecoratingPostProcessor());
		Consumer.MessageHandler handler = startContainer();

		handler.handle(mock(Consumer.Context.class), amqpMessage());

		ArgumentCaptor<Message> received = ArgumentCaptor.forClass(Message.class);
		verify(this.listener).onMessage(received.capture());
		assertThat(received.getValue().getMessageProperties().getHeaders()).containsEntry("processed", Boolean.TRUE);
	}

	@Test
	@SuppressWarnings("unchecked")
	void afterReceivePostProcessorsApplyToBatchListener() {
		this.container.setAfterReceivePostProcessors(new DecoratingPostProcessor());
		this.container.setBatchSize(2);
		this.container.setTaskScheduler(mock(TaskScheduler.class));
		Consumer.MessageHandler handler = startContainer();
		Consumer.Context context = mock();
		Consumer.BatchContext batchContext = mock();
		AtomicInteger batchSize = new AtomicInteger();
		given(batchContext.size()).willAnswer((invocation) -> batchSize.get());
		willAnswer((invocation) -> batchSize.incrementAndGet()).given(batchContext).add(any());
		given(context.batch(2)).willReturn(batchContext);

		handler.handle(context, amqpMessage());
		handler.handle(context, amqpMessage());

		ArgumentCaptor<List<Message>> received = ArgumentCaptor.forClass(List.class);
		verify(this.listener).onMessageBatch(received.capture());
		assertThat(received.getValue())
				.hasSize(2)
				.allSatisfy((message) ->
						assertThat(message.getMessageProperties().getHeaders()).containsEntry("processed", Boolean.TRUE));
	}

	private Consumer.MessageHandler startContainer() {
		this.container.afterPropertiesSet();
		this.container.start();
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

	private static final class DecoratingPostProcessor implements MessagePostProcessor {

		@Override
		public Message postProcessMessage(Message message) {
			message.getMessageProperties().setHeader("processed", Boolean.TRUE);
			return message;
		}

	}

}
