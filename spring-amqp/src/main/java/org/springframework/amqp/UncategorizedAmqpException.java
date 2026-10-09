/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp;

import org.jspecify.annotations.Nullable;

/**
 * A "catch-all" exception type within the AmqpException hierarchy
 * when no more specific cause is known.
 *
 * @author Mark Pollack
 */
@SuppressWarnings("serial")
public class UncategorizedAmqpException extends AmqpException {

	public UncategorizedAmqpException(@Nullable Throwable cause) {
		super(cause);
	}

	public UncategorizedAmqpException(String message, Throwable cause) {
		super(message, cause);
	}

}
