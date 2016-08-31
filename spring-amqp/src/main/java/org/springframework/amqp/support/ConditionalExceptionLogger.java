/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.support;

import org.apache.commons.logging.Log;

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
	void log(Log logger, String message, Throwable t);

}
