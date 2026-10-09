/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException;

/**
 * This exception indicates that a consumer could not be started because none of
 * its queues are available for listening.
 *
 * @author Gary Russell
 * @since 1.3.5
 *
 */
@SuppressWarnings("serial")
public class QueuesNotAvailableException extends FatalListenerStartupException {

	public QueuesNotAvailableException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
