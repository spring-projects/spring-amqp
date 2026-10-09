/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;

/**
 * Exception to be thrown when the reply of a message failed to be sent.
 *
 * @author Stephane Nicoll
 * @author Artem Bilan
 *
 * @since 1.4
 */
@SuppressWarnings("serial")
public class ReplyFailureException extends AmqpException {

	private final @Nullable Address replyTo;

	public ReplyFailureException(String msg, @Nullable Address replyTo, Throwable cause) {
		super(msg, cause);
		this.replyTo = replyTo;
	}

	public @Nullable Address getReplyTo() {
		return this.replyTo;
	}

}
