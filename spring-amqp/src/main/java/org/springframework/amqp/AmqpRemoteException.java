/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp;

/**
 * An exception that wraps an exception thrown by the server in a
 * request/reply scenario.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@SuppressWarnings("serial")
public class AmqpRemoteException extends AmqpException {

	public AmqpRemoteException(Throwable cause) {
		super(cause);
	}

}
