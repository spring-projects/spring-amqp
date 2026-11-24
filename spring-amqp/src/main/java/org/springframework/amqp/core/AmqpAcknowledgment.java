/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
