/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.amqp.core;

import java.io.Serial;

import org.springframework.amqp.AmqpException;

/**
 * Exception thrown if the request message cannot be delivered when the mandatory flag is
 * set.
 *
 * @author Gary Russell
 * @since 1.5
 *
 */
public class AmqpMessageReturnedException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1866579721126554167L;

	private final transient ReturnedMessage returned;

	public AmqpMessageReturnedException(String message, ReturnedMessage returned) {
		super(message);
		this.returned = returned;
	}

	public Message getReturnedMessage() {
		return this.returned.getMessage();
	}

	public int getReplyCode() {
		return this.returned.getReplyCode();
	}

	public String getReplyText() {
		return this.returned.getReplyText();
	}

	public String getExchange() {
		return this.returned.getExchange();
	}

	public String getRoutingKey() {
		return this.returned.getRoutingKey();
	}

	public ReturnedMessage getReturned() {
		return this.returned;
	}

	@Override
	public String toString() {
		return "AmqpMessageReturnedException: "
				+ getMessage()
				+ ", " + this.returned.toString();
	}

}
