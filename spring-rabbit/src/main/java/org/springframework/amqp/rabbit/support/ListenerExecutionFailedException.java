/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.support;

import org.springframework.amqp.core.Message;

/**
 * Exception to be thrown when the execution of a listener method failed.
 *
 * @author Juergen Hoeller
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @deprecated in favor of {@link org.springframework.amqp.listener.ListenerExecutionFailedException}
 */
@Deprecated(forRemoval = true, since = "4.1")
@SuppressWarnings("serial")
public class ListenerExecutionFailedException extends org.springframework.amqp.listener.ListenerExecutionFailedException {

	/**
	 * Constructor for ListenerExecutionFailedException.
	 * @param msg the detail message
	 * @param cause the exception thrown by the listener method
	 * @param failedMessage the message(s) that failed
	 */
	public ListenerExecutionFailedException(String msg, Throwable cause, Message... failedMessage) {
		super(msg, cause, failedMessage);
	}

}
