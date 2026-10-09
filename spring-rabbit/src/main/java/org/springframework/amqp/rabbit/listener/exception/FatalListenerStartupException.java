/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.exception;

import org.springframework.amqp.AmqpException;

/**
 * Exception to be thrown when the execution of a listener method failed on startup.
 *
 * @author Dave Syer
 * @see org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter
 */
@SuppressWarnings("serial")
public class FatalListenerStartupException extends AmqpException {

	/**
	 * Constructor for ListenerExecutionFailedException.
	 * @param msg the detail message
	 * @param cause the exception thrown by the listener method
	 */
	public FatalListenerStartupException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
