/*
 * Copyright 2013-present the original author or authors.
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
