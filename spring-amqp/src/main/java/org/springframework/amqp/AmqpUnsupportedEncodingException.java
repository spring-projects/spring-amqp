/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp;

/**
 * RuntimeException for unsupported encoding in an AMQP operation.
 *
 * @author Mark Pollack
 */
@SuppressWarnings("serial")
public class AmqpUnsupportedEncodingException extends AmqpException {

	public AmqpUnsupportedEncodingException(Throwable cause) {
		super(cause);
	}

}
