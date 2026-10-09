/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp;

import org.jspecify.annotations.Nullable;

/**
 * Base RuntimeException for errors that occur when executing AMQP operations.
 *
 * @author Mark Fisher
 * @author Artem Bilan
 */
@SuppressWarnings("serial")
public class AmqpException extends RuntimeException {

	public AmqpException(String message) {
		super(message);
	}

	public AmqpException(@Nullable Throwable cause) {
		super(cause);
	}

	public AmqpException(@Nullable String message, @Nullable Throwable cause) {
		super(message, cause);
	}

}
