/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.rabbit.stream.retry;

import com.rabbitmq.stream.MessageHandler.Context;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;

/**
 * Implementations of this interface can handle failed messages after retries are
 * exhausted.
 *
 * @author Gary Russell
 * @since 2.4.5
 *
 */
@FunctionalInterface
public interface StreamMessageRecoverer extends MessageRecoverer {

	@Override
	default void recover(Message message, @Nullable Throwable cause) {
	}

	/**
	 * Callback for message that was consumed but failed all retry attempts.
	 *
	 * @param message the message to recover.
	 * @param context the context.
	 * @param cause the cause of the error.
	 */
	void recover(com.rabbitmq.stream.Message message, Context context, Throwable cause);

}
