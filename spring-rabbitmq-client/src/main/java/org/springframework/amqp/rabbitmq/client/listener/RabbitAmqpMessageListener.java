/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client.listener;

import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Consumer.Context;
import com.rabbitmq.client.amqp.Message;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageListener;

/**
 * A message listener that receives native AMQP 1.0 messages from RabbitMQ.
 *
 * @author Artem Bilan
 * @author Seonwoo Jung
 * @author Ngoc Nhan
 *
 * @since 4.0
 */
public interface RabbitAmqpMessageListener extends MessageListener {

	/**
	 * Process an AMQP message.
	 * @param message the message to process.
	 * @param context the consumer context to settle message.
	 *                {@code null} if container is configured for {@code autoSettle}.
	 * @param queueName the queue name to be consumed.
	 */
	void onAmqpMessage(Message message, Consumer.@Nullable Context context, @Nullable String queueName);

	/**
	 * Process an AMQP message, which has already been converted to a Spring AMQP message.
	 * The container calls this method when an advice chain is configured, so advices
	 * (e.g. a stateless retry interceptor with a
	 * {@link org.springframework.amqp.rabbit.retry.MessageRecoverer}) can access the
	 * converted message in the method arguments.
	 * By default, delegates to {@link #onAmqpMessage(Message, Context, String)}.
	 * @param message the message to process.
	 * @param springMessage the {@link org.springframework.amqp.core.Message} converted from the AMQP message.
	 * @param context the consumer context to settle message.
	 *                {@code null} if container is configured for {@code autoSettle}.
	 * @since 4.0.7
	 */
	default void onAmqpMessage(Message message, org.springframework.amqp.core.Message springMessage,
			Consumer.@Nullable Context context) {

		onAmqpMessage(message, context, springMessage.getMessageProperties().getConsumerQueue());
	}

}
