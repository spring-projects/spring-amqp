/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.amqp;

/**
 * The {@link AmqpException} thrown when some resource can't be accessed.
 * For example when {@code channelMax} limit is reached and connect can't
 * create a new channel at the moment.
 *
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.7.7
 */
@SuppressWarnings("serial")
public class AmqpResourceNotAvailableException extends AmqpException {

	public AmqpResourceNotAvailableException(String message) {
		super(message);
	}

	public AmqpResourceNotAvailableException(Throwable cause) {
		super(cause);
	}

	public AmqpResourceNotAvailableException(String message, Throwable cause) {
		super(message, cause);
	}

}
