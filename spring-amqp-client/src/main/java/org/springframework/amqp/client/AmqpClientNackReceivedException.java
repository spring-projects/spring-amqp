/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client;

import java.io.Serial;

import org.apache.qpid.protonj2.client.Message;

import org.springframework.amqp.AmqpException;

/**
 * An exception thrown when a negative acknowledgement is received after publishing a message.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class AmqpClientNackReceivedException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1L;

	private final transient Message<?> failedMessage;

	/**
	 * Create an instance with the provided message and failed message.
	 * @param message the message.
	 * @param failedMessage the failed message.
	 */
	public AmqpClientNackReceivedException(String message, Message<?> failedMessage) {
		super(message);
		this.failedMessage = failedMessage;
	}

	/**
	 * Return the failed message.
	 * @return the message.
	 */
	public Message<?> getFailedMessage() {
		return this.failedMessage;
	}
}
