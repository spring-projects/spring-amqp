/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.listener;

import java.io.Serial;

import org.springframework.amqp.AmqpException;

/**
 * Exception to be thrown when the execution of a listener method failed with an
 * irrecoverable problem as an alternative to the {@link org.springframework.amqp.AmqpRejectAndDontRequeueException}.
 * The listener container must requeue the message and stop its execution in case of this error.
 *
 * @author Dave Syer
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class FatalListenerExecutionException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor for ListenerExecutionFailedException.
	 * @param msg the detail message
	 * @param cause the exception thrown by the listener method
	 */
	public FatalListenerExecutionException(String msg, Throwable cause) {
		super(msg, cause);
	}

	/**
	 * Constructor for ListenerExecutionFailedException.
	 * @param msg the detail message
	 */
	public FatalListenerExecutionException(String msg) {
		super(msg);
	}

}
