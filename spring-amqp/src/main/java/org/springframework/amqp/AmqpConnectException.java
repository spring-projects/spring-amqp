/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp;

import java.net.ConnectException;

/**
 * RuntimeException wrapper for an {@link ConnectException} which can be commonly thrown from AMQP operations if the
 * remote process dies or there is a network issue.
 *
 * @author Dave Syer
 * @author Gary Russell
 */
@SuppressWarnings("serial")
public class AmqpConnectException extends AmqpException {

	/**
	 * Construct an instance with the supplied message and cause.
	 * @param cause the cause.
	 */
	public AmqpConnectException(Exception cause) {
		super(cause);
	}

	/**
	 * Construct an instance with the supplied message and cause.
	 * @param message the message.
	 * @param cause the cause.
	 * @since 2.0
	 */
	public AmqpConnectException(String message, Throwable cause) {
		super(message, cause);
	}

}
