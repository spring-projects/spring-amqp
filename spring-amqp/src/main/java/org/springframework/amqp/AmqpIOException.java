/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp;

import java.io.IOException;

/**
 * RuntimeException wrapper for an {@link IOException} which
 * can be commonly thrown from AMQP operations.
 *
 * @author Mark Pollack
 * @author Gary Russell
 */
@SuppressWarnings("serial")
public class AmqpIOException extends AmqpException {

	/**
	 * Construct an instance with the provided cause.
	 * @param cause the cause.
	 */
	public AmqpIOException(IOException cause) {
		super(cause);
	}

	/**
	 * Construct an instance with the provided message and cause.
	 * @param message the message.
	 * @param cause the cause.
	 * @since 2.0
	 */
	public AmqpIOException(String message, Throwable cause) {
		super(message, cause);
	}

}
