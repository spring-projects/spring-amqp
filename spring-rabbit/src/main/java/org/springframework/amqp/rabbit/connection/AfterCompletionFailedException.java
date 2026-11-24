/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbit.connection;

import java.io.Serial;

import org.springframework.amqp.AmqpException;

/**
 * Represents a failure to commit or rollback when performing afterCompletion
 * after the primary transaction completes.
 *
 * @author Gary Russell
 * @since 2.4
 */
public class AfterCompletionFailedException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1L;

	private final int syncStatus;

	/**
	 * Construct an instance with the provided properties.
	 * @param syncStatus the synchronization status.
	 * @param cause the cause.
	 */
	public AfterCompletionFailedException(int syncStatus, Throwable cause) {
		super(cause);
		this.syncStatus = syncStatus;
	}

	/**
	 * Return the synchronization status.
	 * @return the status.
	 */
	public int getSyncStatus() {
		return this.syncStatus;
	}

}
