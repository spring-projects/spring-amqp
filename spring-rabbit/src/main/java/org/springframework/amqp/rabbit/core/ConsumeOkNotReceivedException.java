/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import org.springframework.amqp.AmqpException;

/**
 * Thrown when a blocking receive operation is performed but the consumeOk
 * was not received before the receive timeout. Consider increasing the
 * receive timeout.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@SuppressWarnings("serial")
public class ConsumeOkNotReceivedException extends AmqpException {

	public ConsumeOkNotReceivedException(String message) {
		super(message);
	}

}
