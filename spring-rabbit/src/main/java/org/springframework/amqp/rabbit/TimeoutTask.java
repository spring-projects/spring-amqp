/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.amqp.rabbit;

import java.util.concurrent.ConcurrentMap;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.AmqpReplyTimeoutException;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;

/**
 * A {@link Runnable} used to time out a {@link RabbitFuture}.
 *
 * @author Gary Russell
 * @since 2.4.7
 */
public class TimeoutTask implements Runnable {

	private final RabbitFuture<?> future;

	private final ConcurrentMap<String, RabbitFuture<?>> pending;

	private final @Nullable DirectReplyToMessageListenerContainer container;

	TimeoutTask(RabbitFuture<?> future, ConcurrentMap<String, RabbitFuture<?>> pending,
			@Nullable DirectReplyToMessageListenerContainer container) {

		this.future = future;
		this.pending = pending;
		this.container = container;
	}

	@Override
	public void run() {
		this.pending.remove(this.future.getCorrelationId());
		ChannelHolder holder = this.future.getChannelHolder();
		if (holder != null && this.container != null) {
			this.container.releaseConsumerFor(holder, false, null); // NOSONAR
		}
		this.future.completeExceptionally(
				new AmqpReplyTimeoutException("Reply timed out", this.future.getRequestMessage()));
	}

}
