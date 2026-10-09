/*
 * Copyright 2025-present the original author or authors.
 */

package org.springframework.amqp.core;

/**
 * An abstraction over acknowledgments.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
@FunctionalInterface
public interface AmqpAcknowledgment {

	/**
	 * Acknowledge the message.
	 * @param status the status.
	 */
	void acknowledge(Status status);

	default void acknowledge() {
		acknowledge(Status.ACCEPT);
	}

	enum Status {

		/**
		 * Mark the message as accepted.
		 */
		ACCEPT,

		/**
		 * Mark the message as rejected.
		 */
		REJECT,

		/**
		 * Reject the message and requeue so that it will be redelivered.
		 */
		REQUEUE

	}

}
