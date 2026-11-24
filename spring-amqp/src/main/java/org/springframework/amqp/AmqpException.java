/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp;

import org.jspecify.annotations.Nullable;

/**
 * Base RuntimeException for errors that occur when executing AMQP operations.
 *
 * @author Mark Fisher
 * @author Artem Bilan
 */
@SuppressWarnings("serial")
public class AmqpException extends RuntimeException {

	public AmqpException(String message) {
		super(message);
	}

	public AmqpException(@Nullable Throwable cause) {
		super(cause);
	}

	public AmqpException(@Nullable String message, @Nullable Throwable cause) {
		super(message, cause);
	}

}
