/*
 * Copyright 2021-present the original author or authors.
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

package org.springframework.rabbit.stream.producer;

import org.springframework.amqp.AmqpException;

/**
 * Used to complete the future exceptionally when sending fails.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public class StreamSendException extends AmqpException {

	private static final long serialVersionUID = 1L;

	private final int confirmationCode;
	/**
	 * Construct an instance with the provided message.
	 * @param message the message.
	 * @param code the confirmation code.
	 */
	public StreamSendException(String message, int code) {
		super(message);
		this.confirmationCode = code;
	}

	/**
	 * Return the confirmation code, if available.
	 * @return the code.
	 */
	public int getConfirmationCode() {
		return this.confirmationCode;
	}

}
