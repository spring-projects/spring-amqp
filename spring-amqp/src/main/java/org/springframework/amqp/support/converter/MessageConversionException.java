/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
