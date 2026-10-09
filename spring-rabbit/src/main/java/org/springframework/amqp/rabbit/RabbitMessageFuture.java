/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.amqp.rabbit;

import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;

/**
 * A {@link RabbitFuture} with a return type of {@link Message}.
 *
 * @author Gary Russell
 * @since 2.4.7
 */
public class RabbitMessageFuture extends RabbitFuture<Message> {

	RabbitMessageFuture(String correlationId, Message requestMessage,
			BiConsumer<String, @Nullable ChannelHolder> canceler,
			Function<RabbitFuture<?>, @Nullable ScheduledFuture<?>> timeoutTaskFunction) {

		super(correlationId, requestMessage, canceler, timeoutTaskFunction);
	}

}
