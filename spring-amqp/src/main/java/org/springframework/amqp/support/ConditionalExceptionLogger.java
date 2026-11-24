/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.support;

import org.apache.commons.logging.Log;
import org.jspecify.annotations.Nullable;

import org.springframework.core.log.LogMessage;

/**
 * For components that support customization of the logging of certain events, users can
 * provide an implementation of this interface to modify the existing logging behavior.
 *
 * @author Gary Russell
 * @since 1.5
 *
 */
@FunctionalInterface
public interface ConditionalExceptionLogger {

	/**
	 * Log the event.
	 * @param logger the logger to use.
	 * @param message a message that the caller suggests should be included in the log.
	 * @param t a throwable; may be null.
	 */
	void log(Log logger, String message, @Nullable Throwable t);

	/**
	 * Log a consumer restart; debug by default.
	 * @param logger the logger.
	 * @param message the message.
	 * @since 3.1
	 */
	default void logRestart(Log logger, LogMessage message) {
		logger.debug(message);
	}

}
