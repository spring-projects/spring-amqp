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

package org.springframework.amqp.rabbit.support;

import java.io.Serial;

/**
 * Thrown when the broker cancels the consumer and the message
 * queue is drained.
 *
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public class ConsumerCancelledException extends RuntimeException {

	@Serial
	private static final long serialVersionUID = 3815997920289066359L;

	public ConsumerCancelledException() {
	}

	public ConsumerCancelledException(Throwable cause) {
		super(cause);
	}

}
