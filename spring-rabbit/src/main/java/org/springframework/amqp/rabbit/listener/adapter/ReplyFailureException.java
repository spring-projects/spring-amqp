/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Address;

/**
 * Exception to be thrown when the reply of a message failed to be sent.
 *
 * @author Stephane Nicoll
 * @author Artem Bilan
 *
 * @since 1.4
 *
 * @deprecated since 4.1 in favor of {@link org.springframework.amqp.listener.adapter.ReplyFailureException}.
 */
@SuppressWarnings("serial")
@Deprecated(since = "4.1", forRemoval = true)
public class ReplyFailureException extends org.springframework.amqp.listener.adapter.ReplyFailureException {

	public ReplyFailureException(String msg, @Nullable Address replyTo, Throwable cause) {
		super(msg, replyTo, cause);
	}

}
