/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
