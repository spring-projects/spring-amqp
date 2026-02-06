/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client.listener;

import org.apache.qpid.protonj2.client.Delivery;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;

/**
 * An error handler which is called when a {code @AmqpListener} method
 * throws an exception. This is invoked higher up the stack than the
 * listener container's error handler.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
@FunctionalInterface
public interface AmqpListenerErrorHandler {

	/**
	 * Handle the error. If an exception is not thrown, the return value is returned to
	 * the sender using normal {@code replyTo/@SendTo} semantics.
	 *
	 * @param delivery  the ProtonJ delivery that failed.
	 * @param message   the converted spring-messaging message (if available).
	 * @param exception the exception the listener threw, wrapped in a {@link ListenerExecutionFailedException}.
	 * @return the return value (if any) to be sent to the sender as error compensation.
	 * @throws Exception an exception which may be the original or different.
	 */
	@Nullable
	Object handleError(Delivery delivery, @Nullable Message<?> message, ListenerExecutionFailedException exception)
			throws Exception;

}
