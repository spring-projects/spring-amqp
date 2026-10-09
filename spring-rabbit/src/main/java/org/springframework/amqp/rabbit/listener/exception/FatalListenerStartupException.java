/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.exception;

/**
 * Exception to be thrown when the execution of a listener method failed on startup.
 *
 * @author Dave Syer
 * @see org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter
 */
@SuppressWarnings("serial")
@Deprecated(since = "4.1", forRemoval = true)
public class FatalListenerStartupException extends org.springframework.amqp.listener.FatalListenerStartupException {

	/**
	 * Constructor for ListenerExecutionFailedException.
	 * @param msg the detail message
	 * @param cause the exception thrown by the listener method
	 */
	public FatalListenerStartupException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
