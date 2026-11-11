/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import java.io.Serial;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;

/**
 * An exception thrown when a negative acknowledgement received after publishing a
 * message.
 *
 * @author Gary Russell
 * @since 2.3.3
 *
 */
public class AmqpNackReceivedException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1L;

	private final Message failedMessage;

	/**
	 * Create an instance with the provided message and failed message.
	 * @param message the message.
	 * @param failedMessage the failed message.
	 */
	public AmqpNackReceivedException(String message, Message failedMessage) {
		this(message, failedMessage, null);
	}

	/**
	 * Create an instance with the provided message, failed message, and cause.
	 * @param message the message.
	 * @param failedMessage the failed message.
	 * @param cause the cause of the negative acknowledgment, if available.
	 * @since 4.0.7
	 */
	public AmqpNackReceivedException(String message, Message failedMessage, @Nullable Throwable cause) {
		super(message, cause);
		this.failedMessage = failedMessage;
	}

	/**
	 * Return the failed message.
	 * @return the message.
	 */
	public Message getFailedMessage() {
		return this.failedMessage;
	}

}
