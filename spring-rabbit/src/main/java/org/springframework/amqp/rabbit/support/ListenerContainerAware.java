/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.amqp.rabbit.support;

import java.util.Collection;

import org.jspecify.annotations.Nullable;

/**
 * {@link org.springframework.amqp.core.MessageListener}s that also implement this
 * interface can have configuration verified during initialization.
 *
 * @author Gary Russell
 * @author Jeongjun Min
 * @since 1.5
 *
 */
@FunctionalInterface
public interface ListenerContainerAware {

	/**
	 * Return the queue names that the listener expects to listen to.
	 *
	 * @return the queue names.
	 */
	@Nullable
	Collection<String> expectedQueueNames();

	/**
	 * Return a counter for pending replies, if any.
	 * @return the counter, or null.
	 * @since 4.0
	 */
	default @Nullable ActiveObjectCounter<Object> getPendingReplyCounter() {
		return null;
	}
}
