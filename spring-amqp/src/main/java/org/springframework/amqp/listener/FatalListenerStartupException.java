/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.listener;

import java.io.Serial;

import org.springframework.amqp.AmqpException;

/**
 * Exception to be thrown when the execution of a listener method failed on startup.
 *
 * @author Dave Syer
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class FatalListenerStartupException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor for ListenerExecutionFailedException.
	 * @param msg the detail message
	 * @param cause the exception thrown by the listener method
	 */
	public FatalListenerStartupException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
