/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp;

/**
 * Thrown when the connection factory has been destroyed during
 * context close; the factory can no longer open connections.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@SuppressWarnings("serial")
public class AmqpApplicationContextClosedException extends AmqpException {

	public AmqpApplicationContextClosedException(String message) {
		super(message);
	}

}
