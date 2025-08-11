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

package org.springframework.amqp.rabbit.retry;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;

/**
 * Callback for stateful retry after the retry policy has exhausted.
 *
 * @author Stephane Nicoll
 */
@FunctionalInterface
public interface MessageRecoveryCallback {

	/**
	 * Recover the processing of the given {@link Message}.
	 * @param message the message to process
	 * @param replyTo the replyTo {@link Address}
	 * @param cause the last exception thrown
	 * @return an Object that can be used to replace the result of message processing
	 */
	@Nullable
	Object recover(Message message, Address replyTo, Throwable cause);

}
