/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.rabbit.stream.producer;

import java.io.Serial;

import org.springframework.amqp.AmqpException;

/**
 * Used to complete the future exceptionally when sending fails.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public class StreamSendException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1L;

	private final int confirmationCode;
	/**
	 * Construct an instance with the provided message.
	 * @param message the message.
	 * @param code the confirmation code.
	 */
	public StreamSendException(String message, int code) {
		super(message);
		this.confirmationCode = code;
	}

	/**
	 * Return the confirmation code, if available.
	 * @return the code.
	 */
	public int getConfirmationCode() {
		return this.confirmationCode;
	}

}
