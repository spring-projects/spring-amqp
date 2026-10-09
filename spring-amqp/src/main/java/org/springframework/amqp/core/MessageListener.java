/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.List;

/**
 * Listener interface to receive asynchronous delivery of Amqp Messages.
 *
 * @author Mark Pollack
 * @author Gary Russell
 */
@FunctionalInterface
public interface MessageListener {

	/**
	 * Delivers a single message.
	 * @param message the message.
	 */
	void onMessage(Message message);

	/**
	 * Called by the container to inform the listener of its acknowledgement
	 * mode.
	 * @param mode the {@link AcknowledgeMode}.
	 * @since 2.1.4
	 */
	default void containerAckMode(AcknowledgeMode mode) {
		// NOSONAR - empty
	}

	/**
	 * Return true if this listener is request/reply and the replies are
	 * async.
	 * @return true for async replies.
	 * @since 2.2.21
	 */
	default boolean isAsyncReplies() {
		return false;
	}

	/**
	 * Delivers a batch of messages.
	 * @param messages the messages.
	 * @since 2.2
	 */
	default void onMessageBatch(List<Message> messages) {
		throw new UnsupportedOperationException("This listener does not support message batches");
	}

}
