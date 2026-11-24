/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbit.retry;

import org.springframework.amqp.core.Message;

/**
 * Implementations of this interface can handle failed messages after retries are
 * exhausted.
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
@FunctionalInterface
public interface MessageRecoverer {

	/**
	 * Callback for a message that was consumed but failed all retry attempts.
	 * @param message the message to recover
	 * @param cause the cause of the error
	 */
	void recover(Message message, Throwable cause);

}
