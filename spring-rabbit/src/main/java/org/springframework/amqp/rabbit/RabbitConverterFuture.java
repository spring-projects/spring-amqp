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
import org.springframework.core.ParameterizedTypeReference;

/**
 * A {@link RabbitFuture} with a return type of the template's
 * generic parameter.
 * @param <C> the type.
 *
 * @author Gary Russell
 * @since 2.4.7
 */
public class RabbitConverterFuture<C> extends RabbitFuture<C> {

	private volatile @Nullable ParameterizedTypeReference<C> returnType;

	RabbitConverterFuture(String correlationId, Message requestMessage,
			BiConsumer<String, @Nullable ChannelHolder> canceler,
			Function<RabbitFuture<?>, @Nullable ScheduledFuture<?>> timeoutTaskFunction) {

		super(correlationId, requestMessage, canceler, timeoutTaskFunction);
	}

	public @Nullable ParameterizedTypeReference<C> getReturnType() {
		return this.returnType;
	}

	public void setReturnType(@Nullable ParameterizedTypeReference<C> returnType) {
		this.returnType = returnType;
	}

}
