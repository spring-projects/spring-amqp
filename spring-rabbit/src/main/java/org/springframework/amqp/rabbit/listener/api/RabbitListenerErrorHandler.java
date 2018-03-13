/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.amqp.rabbit.listener.api;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;

/**
 * An error handler which is called when a {code @RabbitListener} method
 * throws an exception. This is invoked higher up the stack than the
 * listener container's error handler.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@FunctionalInterface
public interface RabbitListenerErrorHandler {

	/**
	 * Handle the error. If an exception is not thrown, the return value is returned to
	 * the sender using normal {@code replyTo/@SendTo} semantics.
	 * @param amqpMessage the raw message received.
	 * @param message the converted spring-messaging message.
	 * @param exception the exception the listener threw, wrapped in a
	 * {@link ListenerExecutionFailedException}.
	 * @return the return value to be sent to the sender.
	 * @throws Exception an exception which may be the original or different.
	 */
	Object handleError(Message amqpMessage, org.springframework.messaging.Message<?> message,
			ListenerExecutionFailedException exception) throws Exception;

}
