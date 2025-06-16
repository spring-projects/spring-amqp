/*
 * Copyright 2018-present the original author or authors.
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
public final class SendRetryContextAccessor {

	/**
	 * Key for the message we tried to send.
	 */
	public static final String MESSAGE = "message";

	/**
	 * Key for the Address we tried to send to.
	 */
	public static final String ADDRESS = "address";

	private SendRetryContextAccessor() {
	}

	/**
	 * Retrieve the {@link Message} from the context.
	 * @param context the context.
	 * @return the message.
	 * @see #MESSAGE
	 */
	public static Message getMessage(RetryContext context) {
		return (Message) context.getAttribute(MESSAGE);
	}

	/**
	 * Retrieve the {@link Address} from the context.
	 * @param context the context.
	 * @return the address.
	 * @see #ADDRESS
	 */
	public static Address getAddress(RetryContext context) {
		return (Address) context.getAttribute(ADDRESS);
	}

}
