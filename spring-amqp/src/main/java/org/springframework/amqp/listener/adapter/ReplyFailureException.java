/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.listener.adapter;

import java.io.Serial;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;

/**
 * Exception to be thrown when the reply of a message failed to be sent.
 *
 * @author Stephane Nicoll
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class ReplyFailureException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1L;

	private final @Nullable Address replyTo;

	public ReplyFailureException(String msg, @Nullable Address replyTo, Throwable cause) {
		super(msg, cause);
		this.replyTo = replyTo;
	}

	public @Nullable Address getReplyTo() {
		return this.replyTo;
	}

}
