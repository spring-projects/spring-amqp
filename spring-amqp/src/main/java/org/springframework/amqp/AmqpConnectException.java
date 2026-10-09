/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp;

/**
 * RuntimeException wrapper for an {@link java.net.ConnectException} which can be commonly
 * thrown from AMQP operations if the remote process dies or there is a network issue.
 *
 * @author Dave Syer
 * @author Gary Russell
 */
@SuppressWarnings("serial")
public class AmqpConnectException extends AmqpException {

	/**
	 * Construct an instance with the supplied message and cause.
	 * @param cause the cause.
	 */
	public AmqpConnectException(Exception cause) {
		super(cause);
	}

	/**
	 * Construct an instance with the supplied message and cause.
	 * @param message the message.
	 * @param cause the cause.
	 * @since 2.0
	 */
	public AmqpConnectException(String message, Throwable cause) {
		super(message, cause);
	}

}
