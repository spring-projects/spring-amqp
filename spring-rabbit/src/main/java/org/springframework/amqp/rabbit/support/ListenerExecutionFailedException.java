/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.support;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;

/**
 * Exception to be thrown when the execution of a listener method failed.
 *
 * @author Juergen Hoeller
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @see org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter
 */
@SuppressWarnings("serial")
public class ListenerExecutionFailedException extends AmqpException {

	private final List<Message> failedMessages;

	/**
	 * Constructor for ListenerExecutionFailedException.
	 * @param msg the detail message
	 * @param cause the exception thrown by the listener method
	 * @param failedMessage the message(s) that failed
	 */
	public ListenerExecutionFailedException(String msg, Throwable cause, Message... failedMessage) {
		super(msg, cause);
		this.failedMessages = Arrays.asList(failedMessage);
	}

	public @Nullable Message getFailedMessage() {
		return this.failedMessages.isEmpty() ? null : this.failedMessages.get(0);
	}

	public Collection<Message> getFailedMessages() {
		return Collections.unmodifiableList(this.failedMessages);
	}

}
