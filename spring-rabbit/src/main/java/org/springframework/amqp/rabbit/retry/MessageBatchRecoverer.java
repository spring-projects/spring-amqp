/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.rabbit.retry;

import java.util.List;

import org.springframework.amqp.core.Message;

/**
 * A retry recoverer for use with a batch listener. Users should consider throwing an
 * exception containing the index within the batch where the exception occurred, allowing
 * the recoverer to properly recover the remaining records.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.2
 *
 */
@FunctionalInterface
public interface MessageBatchRecoverer extends MessageRecoverer {

	@Override
	default void recover(Message message, Throwable cause) {
		throw new IllegalStateException("MessageBatchRecoverer configured with a non-batch listener");
	}

	/**
	 * Callback for a message batch that was consumed but failed all retry attempts.
	 * @param messages the messages to recover
	 * @param cause the cause of the error
	 */
	void recover(List<Message> messages, Throwable cause);

}
