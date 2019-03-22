/*
 * Copyright 2018-2019 the original author or authors.
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

/**
 * The {@link AmqpException} thrown when some resource can't be accessed.
 * For example when {@code channelMax} limit is reached and connect can't
 * create a new channel at the moment.
 *
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.7.7
 */
@SuppressWarnings("serial")
public class AmqpResourceNotAvailableException extends AmqpException {

	public AmqpResourceNotAvailableException(String message) {
		super(message);
	}

	public AmqpResourceNotAvailableException(Throwable cause) {
		super(cause);
	}

	public AmqpResourceNotAvailableException(String message, Throwable cause) {
		super(message, cause);
	}

}
