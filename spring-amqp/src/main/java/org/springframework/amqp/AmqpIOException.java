/*
 * Copyright 2002-present the original author or authors.
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

import java.io.IOException;

/**
 * RuntimeException wrapper for an {@link IOException} which
 * can be commonly thrown from AMQP operations.
 *
 * @author Mark Pollack
 * @author Gary Russell
 */
@SuppressWarnings("serial")
public class AmqpIOException extends AmqpException {

	/**
	 * Construct an instance with the provided cause.
	 * @param cause the cause.
	 */
	public AmqpIOException(IOException cause) {
		super(cause);
	}

	/**
	 * Construct an instance with the provided message and cause.
	 * @param message the message.
	 * @param cause the cause.
	 * @since 2.0
	 */
	public AmqpIOException(String message, Throwable cause) {
		super(message, cause);
	}

}
