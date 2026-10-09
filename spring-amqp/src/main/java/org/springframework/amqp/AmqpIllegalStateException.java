/*
 * Copyright 2002-present the original author or authors.
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
