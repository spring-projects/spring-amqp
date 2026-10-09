/*
 * Copyright 2016-present the original author or authors.
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
