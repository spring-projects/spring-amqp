/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp;

import org.jspecify.annotations.Nullable;

/**
 * A "catch-all" exception type within the AmqpException hierarchy
 * when no more specific cause is known.
 *
 * @author Mark Pollack
 */
@SuppressWarnings("serial")
public class UncategorizedAmqpException extends AmqpException {

	public UncategorizedAmqpException(@Nullable Throwable cause) {
		super(cause);
	}

	public UncategorizedAmqpException(String message, Throwable cause) {
		super(message, cause);
	}

}
