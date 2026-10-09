/*
 * Copyright 2016-present the original author or authors.
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
