/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.amqp;

/**
 * The special {@link AmqpException} to be thrown from the listener (e.g. retry recoverer callback)
 * to {@code requeue} failed message.
 *
 * @author Artem Bilan
 *
 * @since 2.1
 */
@SuppressWarnings("serial")
public class ImmediateRequeueAmqpException extends AmqpException {

	public ImmediateRequeueAmqpException(String message) {
		super(message);
	}

	public ImmediateRequeueAmqpException(Throwable cause) {
		super(cause);
	}

	public ImmediateRequeueAmqpException(String message, Throwable cause) {
		super(message, cause);
	}

}
