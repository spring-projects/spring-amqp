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

package org.springframework.amqp.rabbit.retry;

import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.util.Assert;

/**
 * MessageRecover that causes the listener container to reject
 * the message without requeuing. This enables failed messages
 * to be sent to a Dead Letter Exchange/Queue, if the broker is
 * so configured.
 *
 * @author Gary Russell
 * @since 1.1.2
 *
 */
public class RejectAndDontRequeueRecoverer implements MessageRecoverer {

	private final Supplier<String> messageSupplier;

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR protected

	/**
	 * Construct an instance with the default exception message.
	 */
	public RejectAndDontRequeueRecoverer() {
		this(() -> "Retry Policy Exhausted");
	}

	/**
	 * Construct an instance with the provided exception message.
	 * @param message the message.
	 * @since 2.3.7
	 */
	public RejectAndDontRequeueRecoverer(String message) {
		this(() -> message);
	}

	/**
	 * Construct an instance with the provided exception message supplier.
	 * @param messageSupplier the message supplier.
	 * @since 2.3.7
	 */
	public RejectAndDontRequeueRecoverer(Supplier<String> messageSupplier) {
		Assert.notNull(messageSupplier, "'messageSupplier' cannot be null");
		this.messageSupplier = messageSupplier;
	}

	@Override
	public void recover(Message message, Throwable cause) {
		if (this.logger.isWarnEnabled()) {
			this.logger.warn("Retries exhausted for message " + message, cause);
		}
		throw new ListenerExecutionFailedException(this.messageSupplier.get(),
					new AmqpRejectAndDontRequeueException(cause), message);
	}

}
