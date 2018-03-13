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

package org.springframework.amqp.rabbit.listener;

import org.apache.commons.logging.Log;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;

/**
 * Utility methods for listener containers.
 *
 * @author Gary Russell
 *
 * @since 2.1
 *
 */
public final class ContainerUtils {

	private ContainerUtils() {
		super();
	}

	/**
	 * Determine whether a message should be requeued; returns true if the throwable is a
	 * {@link MessageRejectedWhileStoppingException} or defaultRequeueRejected is true and
	 * there is not an {@link AmqpRejectAndDontRequeueException} in the cause chain.
	 * @param defaultRequeueRejected the default requeue rejected.
	 * @param throwable the throwable.
	 * @param logger the logger to use for debug.
	 * @return true to requeue.
	 */
	public static boolean shouldRequeue(boolean defaultRequeueRejected, Throwable throwable, Log logger) {
		boolean shouldRequeue = defaultRequeueRejected ||
				throwable instanceof MessageRejectedWhileStoppingException;
		Throwable t = throwable;
		while (shouldRequeue && t != null) {
			if (t instanceof AmqpRejectAndDontRequeueException) {
				shouldRequeue = false;
			}
			t = t.getCause();
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Rejecting messages (requeue=" + shouldRequeue + ")");
		}
		return shouldRequeue;
	}

}
