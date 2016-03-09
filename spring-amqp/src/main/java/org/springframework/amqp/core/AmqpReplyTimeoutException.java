/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.amqp.core;

import org.springframework.amqp.AmqpException;

/**
 * Async reply timeout.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class AmqpReplyTimeoutException extends AmqpException {

	private static final long serialVersionUID = -1981828828502336667L;

	private final Message requestMessage;

	public AmqpReplyTimeoutException(String message, Message requestMessage) {
		super(message);
		this.requestMessage = requestMessage;
	}

	public Message getRequestMessage() {
		return this.requestMessage;
	}

	@Override
	public String toString() {
		return "AmqpReplyTimeoutException [" + getMessage() + ", requestMessage=" + this.requestMessage + "]";
	}

}
