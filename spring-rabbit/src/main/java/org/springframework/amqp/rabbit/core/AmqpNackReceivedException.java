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

package org.springframework.amqp.rabbit.core;

import java.io.Serial;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;

/**
 * An exception thrown when a negative acknowledgement received after publishing a
 * message.
 *
 * @author Gary Russell
 * @since 2.3.3
 *
 */
public class AmqpNackReceivedException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1L;

	private final Message failedMessage;

	/**
	 * Create an instance with the provided message and failed message.
	 * @param message the message.
	 * @param failedMessage the failed message.
	 */
	public AmqpNackReceivedException(String message, Message failedMessage) {
		super(message);
		this.failedMessage = failedMessage;
	}

	/**
	 * Return the failed message.
	 * @return the message.
	 */
	public Message getFailedMessage() {
		return this.failedMessage;
	}

}
