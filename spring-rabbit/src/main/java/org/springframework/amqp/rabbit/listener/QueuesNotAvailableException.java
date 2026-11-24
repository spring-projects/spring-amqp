/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
