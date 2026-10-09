/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.listener;

import java.io.Serial;

import org.springframework.amqp.AmqpException;

/**
 * Exception class that indicates a rejected message on shutdown. Used to trigger a rollback for an
 * external transaction manager in that case.
 *
 * @author Dave Syer
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class MessageRejectedWhileStoppingException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1L;

	public MessageRejectedWhileStoppingException() {
		super("Message listener container was stopping when a message was received");
	}

}
