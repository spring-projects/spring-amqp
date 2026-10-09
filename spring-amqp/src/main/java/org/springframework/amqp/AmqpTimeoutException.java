/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.amqp;

/**
 * Exception thrown when some time-bound operation fails to execute in the
 * desired time.
 *
 * @author Gary Russell
 * @since 1.4.2
 *
 */
public class AmqpTimeoutException extends AmqpException {

	private static final long serialVersionUID = -1981629885617675621L;

	public AmqpTimeoutException(String message, Throwable cause) {
		super(message, cause);
	}

	public AmqpTimeoutException(String message) {
		super(message);
	}

	public AmqpTimeoutException(Throwable cause) {
		super(cause);
	}

}
