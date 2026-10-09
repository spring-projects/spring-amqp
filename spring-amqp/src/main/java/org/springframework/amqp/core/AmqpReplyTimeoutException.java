/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.core;

import org.springframework.amqp.AmqpException;

/**
 * Async reply timeout.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class AmqpReplyTimeoutException extends AmqpException {

	private static final long serialVersionUID = -1981828828502336667L;

	private final Message requestMessage;

	public AmqpReplyTimeoutException(String message, Message requestMessage) {
		super(message);
		this.requestMessage = requestMessage;
	}

	public Message getRequestMessage() {
		return this.requestMessage;
	}

	@Override
	public String toString() {
		return "AmqpReplyTimeoutException [" + getMessage() + ", requestMessage=" + this.requestMessage + "]";
	}

}
