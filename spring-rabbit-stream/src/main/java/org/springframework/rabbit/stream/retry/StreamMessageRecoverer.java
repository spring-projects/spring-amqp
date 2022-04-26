/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.rabbit.stream.retry;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;

import com.rabbitmq.stream.MessageHandler.Context;

/**
 * Implementations of this interface can handle failed messages after retries are
 * exhausted.
 *
 * @author Gary Russell
 * @since 2.4.5
 *
 */
@FunctionalInterface
public interface StreamMessageRecoverer extends MessageRecoverer {

	@Override
	default void recover(Message message, Throwable cause) {
	}

	/**
	 * Callback for message that was consumed but failed all retry attempts.
	 *
	 * @param message the message to recover.
	 * @param context the context.
	 * @param cause the cause of the error.
	 */
	void recover(com.rabbitmq.stream.Message message, Context context, Throwable cause);

}
