/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp;

/**
 * Special exception for listener implementations that want to signal that the current
 * batch of messages should be acknowledged immediately (i.e. as soon as possible) without
 * rollback, and without consuming any more messages within the current transaction.
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 */
@SuppressWarnings("serial")
public class ImmediateAcknowledgeAmqpException extends AmqpException {

	public ImmediateAcknowledgeAmqpException(String message) {
		super(message);
	}

	public ImmediateAcknowledgeAmqpException(Throwable cause) {
		super(cause);
	}

	public ImmediateAcknowledgeAmqpException(String message, Throwable cause) {
		super(message, cause);
	}

}
