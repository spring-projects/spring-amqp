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

package org.springframework.amqp.listener;

import org.apache.commons.logging.Log;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.ImmediateRequeueAmqpException;

/**
 * Utility methods for listener containers.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 4.1
 *
 */
public final class ContainerUtils {

	private ContainerUtils() {
	}

	/**
	 * Determine whether a message should be requeued; returns true if the throwable is a
	 * {@link MessageRejectedWhileStoppingException} or defaultRequeueRejected is true and
	 * there is not an {@link AmqpRejectAndDontRequeueException} in the cause chain or if
	 * there is an {@link ImmediateRequeueAmqpException} in the cause chain.
	 * @param defaultRequeueRejected the default requeue rejected.
	 * @param throwable the throwable.
	 * @param logger the logger to use for debug.
	 * @return true to requeue.
	 */
	public static boolean shouldRequeue(boolean defaultRequeueRejected, Throwable throwable, Log logger) {
		boolean shouldRequeue = defaultRequeueRejected ||
				throwable instanceof MessageRejectedWhileStoppingException;
		Throwable t = throwable;
		while (t != null) {
			if (t instanceof AmqpRejectAndDontRequeueException) {
				shouldRequeue = false;
				break;
			}
			else if (t instanceof ImmediateRequeueAmqpException) {
				shouldRequeue = true;
				break;
			}
			Throwable cause = t.getCause();
			if (cause == t) {
				break;
			}
			t = cause;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Rejecting messages (requeue=" + shouldRequeue + ")");
		}
		return shouldRequeue;
	}

	/**
	 * Return true for {@link AmqpRejectAndDontRequeueException#isRejectManual()}.
	 * @param ex the exception.
	 * @return the exception's rejectManual property, if it's an
	 * {@link AmqpRejectAndDontRequeueException}.
	 */
	public static boolean isRejectManual(Throwable ex) {
		AmqpRejectAndDontRequeueException amqpRejectAndDontRequeueException =
				findInCause(ex, AmqpRejectAndDontRequeueException.class);
		return amqpRejectAndDontRequeueException != null && amqpRejectAndDontRequeueException.isRejectManual();
	}

	/**
	 * Return true for {@link ImmediateAcknowledgeAmqpException}.
	 * @param ex the exception to traverse.
	 * @return true if an {@link ImmediateAcknowledgeAmqpException} is present in the cause chain.
	 */
	public static boolean isImmediateAcknowledge(Throwable ex) {
		return findInCause(ex, ImmediateAcknowledgeAmqpException.class) != null;
	}

	/**
	 * Return true for {@link AmqpRejectAndDontRequeueException}.
	 * @param ex the exception to traverse.
	 * @return true if an {@link AmqpRejectAndDontRequeueException} is present in the cause chain.
	 */
	public static boolean isAmqpReject(Throwable ex) {
		return findInCause(ex, AmqpRejectAndDontRequeueException.class) != null;
	}

	@SuppressWarnings("unchecked")
	private static <T extends Throwable> @Nullable T findInCause(Throwable throwable, Class<T> exceptionToFind) {
		if (exceptionToFind.isAssignableFrom(throwable.getClass())) {
			return (T) throwable;
		}
		Throwable cause = throwable.getCause();
		if (cause == null || cause == throwable) {
			return null;
		}
		return findInCause(cause, exceptionToFind);
	}

}
