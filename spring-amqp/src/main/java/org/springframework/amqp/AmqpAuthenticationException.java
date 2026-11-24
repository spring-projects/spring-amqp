/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp;

/**
 * Runtime wrapper for an authentication exception.
 *
 * @author Gary Russell
 * @since 1.2.1
 *
 */
@SuppressWarnings("serial")
public class AmqpAuthenticationException extends AmqpException {

	public AmqpAuthenticationException(Throwable cause) {
		super(cause);
	}

}
