/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.support;

import org.apache.commons.logging.Log;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.listener.MessageRejectedWhileStoppingException;

/**
 * Utility methods for listener containers.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1
 *
 * @deprecated in favor of {@link org.springframework.amqp.listener.ContainerUtils}
 */
@Deprecated(forRemoval = true, since = "4.1")
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
		return org.springframework.amqp.listener.ContainerUtils.shouldRequeue(defaultRequeueRejected, throwable, logger);
	}

	/**
	 * Return true for {@link AmqpRejectAndDontRequeueException#isRejectManual()}.
	 * @param ex the exception.
	 * @return the exception's rejectManual property, if it's an
	 * {@link AmqpRejectAndDontRequeueException}.
	 * @since 2.2
	 */
	public static boolean isRejectManual(Throwable ex) {
		return org.springframework.amqp.listener.ContainerUtils.isRejectManual(ex);
	}

	/**
	 * Return true for {@link ImmediateAcknowledgeAmqpException}.
	 * @param ex the exception to traverse.
	 * @return true if an {@link ImmediateAcknowledgeAmqpException} is present in the cause chain.
	 * @since 4.0
	 */
	public static boolean isImmediateAcknowledge(Throwable ex) {
		return org.springframework.amqp.listener.ContainerUtils.isImmediateAcknowledge(ex);
	}

	/**
	 * Return true for {@link AmqpRejectAndDontRequeueException}.
	 * @param ex the exception to traverse.
	 * @return true if an {@link AmqpRejectAndDontRequeueException} is present in the cause chain.
	 * @since 4.0
	 */
	public static boolean isAmqpReject(Throwable ex) {
		return org.springframework.amqp.listener.ContainerUtils.isAmqpReject(ex);
	}

}
