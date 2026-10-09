/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.support;

import java.io.Serial;

/**
 * Thrown when the broker cancels the consumer and the message
 * queue is drained.
 *
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public class ConsumerCancelledException extends RuntimeException {

	@Serial
	private static final long serialVersionUID = 3815997920289066359L;

	public ConsumerCancelledException() {
	}

	public ConsumerCancelledException(Throwable cause) {
		super(cause);
	}

}
