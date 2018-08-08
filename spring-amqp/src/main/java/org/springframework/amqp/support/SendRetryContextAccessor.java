/*
 * Copyright 2018 the original author or authors.
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

import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.retry.RetryContext;

/**
 * Type safe accessor for retried message sending.
 *
 * @author Gary Russell
 * @since 2.0.6
 *
 */
public class SendRetryContextAccessor {

	/**
	 * Key for the message we tried to send.
	 */
	public static final String MESSAGE = "message";

	/**
	 * Key for the Address we tried to send to.
	 */
	public static final String ADDRESS = "address";

	private final RetryContext context;

	public SendRetryContextAccessor(RetryContext context) {
		this.context = context;
	}

	public void setMessage(Message message) {
		this.context.setAttribute(MESSAGE, message);
	}

	public Message getMessage() {
		return (Message) this.context.getAttribute(MESSAGE);
	}

	public void setAddress(Address address) {
		this.context.setAttribute(ADDRESS, address);
	}

	public Address getAddress() {
		return (Address) this.context.getAttribute(ADDRESS);
	}

}
