/*
 * Copyright 2002-2025 the original author or authors.
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

package org.springframework.amqp;

import org.jspecify.annotations.Nullable;

/**
 * Exception for listener implementations used to indicate the
 * {@code basic.reject} will be sent with {@code requeue=false}
 * in order to enable features such as DLQ.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.0.1
 *
 */
@SuppressWarnings("serial")
public class AmqpRejectAndDontRequeueException extends AmqpException {

	private final boolean rejectManual;

	/**
	 * Construct an instance with the supplied argument.
	 * @param message A message describing the problem.
	 */
	public AmqpRejectAndDontRequeueException(String message) {
		this(message, false, null);
	}

	/**
	 * Construct an instance with the supplied argument.
	 * @param cause the cause.
	 */
	public AmqpRejectAndDontRequeueException(Throwable cause) {
		this(null, false, cause);
	}

	/**
	 * Construct an instance with the supplied arguments.
	 * @param message A message describing the problem.
	 * @param cause the cause.
	 */
	public AmqpRejectAndDontRequeueException(String message, Throwable cause) {
		this(message, false, cause);
	}

	/**
	 * Construct an instance with the supplied arguments.
	 * @param message A message describing the problem.
	 * @param rejectManual true to reject the message, even with Manual Acks if this is
	 * the top-level exception (e.g. thrown by an error handler).
	 * @param cause the cause.
	 * @since 2.1.9
	 */
	public AmqpRejectAndDontRequeueException(@Nullable String message, boolean rejectManual,
			@Nullable Throwable cause) {

		super(message, cause);
		this.rejectManual = rejectManual;
	}

	/**
	 * True if the container should reject the message, even with manual acks.
	 * @return true to reject.
	 */
	public boolean isRejectManual() {
		return this.rejectManual;
	}

}
