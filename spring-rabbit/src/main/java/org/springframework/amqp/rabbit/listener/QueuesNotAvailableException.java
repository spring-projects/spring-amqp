/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import java.io.Serial;

/**
 * This exception indicates that a consumer could not be started because none of
 * its queues are available for listening.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3.5
 *
 */
@SuppressWarnings("removal")
public class QueuesNotAvailableException
		extends org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException {

	@Serial
	private static final long serialVersionUID = 1L;

	public QueuesNotAvailableException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
