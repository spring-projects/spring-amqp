/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp;

/**
 * Equivalent of an IllegalStateException but within the AmqpException hierarchy.
 *
 * @author Mark Pollack
 */
@SuppressWarnings("serial")
public class AmqpIllegalStateException extends AmqpException {

	public AmqpIllegalStateException(String message) {
		super(message);
	}

	public AmqpIllegalStateException(String message, Throwable cause) {
		super(message, cause);
	}

}
