/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.listener;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

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
 * @since 4.1
 */
public class ListenerExecutionFailedException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1L;

	private final ArrayList<Message> failedMessages;

	/**
	 * Constructor for ListenerExecutionFailedException.
	 * @param msg the detail message
	 * @param cause the exception thrown by the listener method
	 * @param failedMessage the message(s) that failed
	 */
	public ListenerExecutionFailedException(String msg, Throwable cause, Message... failedMessage) {
		super(msg, cause);
		this.failedMessages = new ArrayList<>(Arrays.asList(failedMessage));
	}

	public @Nullable Message getFailedMessage() {
		return this.failedMessages.isEmpty() ? null : this.failedMessages.get(0);
	}

	public Collection<Message> getFailedMessages() {
		return Collections.unmodifiableList(this.failedMessages);
	}

}
