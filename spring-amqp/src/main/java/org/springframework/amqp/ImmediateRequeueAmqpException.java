/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp;

/**
 * The special {@link AmqpException} to be thrown from the listener (e.g. retry recoverer callback)
 * to {@code requeue} failed message.
 *
 * @author Artem Bilan
 *
 * @since 2.1
 */
@SuppressWarnings("serial")
public class ImmediateRequeueAmqpException extends AmqpException {

	public ImmediateRequeueAmqpException(String message) {
		super(message);
	}

	public ImmediateRequeueAmqpException(Throwable cause) {
		super(cause);
	}

	public ImmediateRequeueAmqpException(String message, Throwable cause) {
		super(message, cause);
	}

}
