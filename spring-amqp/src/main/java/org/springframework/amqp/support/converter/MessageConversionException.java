/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.support.converter;

import org.springframework.amqp.AmqpException;

/**
 * <p>
 * Exception to be thrown by message converters if they encounter a problem with converting a message or object.
 * </p>
 * <p>
 * N.B. this is <em>not</em> an {@link AmqpException} because it is a client exception, not a protocol or broker
 * problem.
 * </p>
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 */
@SuppressWarnings("serial")
public class MessageConversionException extends AmqpException {

	public MessageConversionException(String message, Throwable cause) {
		super(message, cause);
	}

	public MessageConversionException(String message) {
		super(message);
	}

}
