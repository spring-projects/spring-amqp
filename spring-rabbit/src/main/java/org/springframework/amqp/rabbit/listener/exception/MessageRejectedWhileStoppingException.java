/*
 * Copyright 2010-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.exception;

import org.springframework.amqp.AmqpException;

/**
 * Exception class that indicates a rejected message on shutdown. Used to trigger a rollback for an
 * external transaction manager in that case.
 */
@SuppressWarnings("serial")
public class MessageRejectedWhileStoppingException extends AmqpException {

	public MessageRejectedWhileStoppingException() {
		super("Message listener container was stopping when a message was received");
	}

}
