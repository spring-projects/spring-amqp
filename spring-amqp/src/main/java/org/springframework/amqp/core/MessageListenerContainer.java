/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.core;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;

/**
 * Internal abstraction used by the framework representing a message
 * listener container. Not meant to be implemented externally.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 4.1
 */
public interface MessageListenerContainer extends SmartLifecycle, InitializingBean {

	/**
	 * Set up the message listener to use. Throws an {@link IllegalArgumentException}
	 * if that message listener type is not supported.
	 * @param messageListener the {@code object} to wrapped to the {@code MessageListener}.
	 */
	void setupMessageListener(MessageListener messageListener);

	/**
	 * Do not check for missing or mismatched queues during startup. Used for lazily
	 * loaded message listener containers to avoid a deadlock when starting such
	 * containers. Applications lazily loading containers should verify the queue
	 * configuration before loading the container bean.
	 */
	default void lazyLoad() {
		// no-op
	}

	/**
	 * Return true if this container is capable of (and configured to) create batches
	 * of consumed messages.
	 * @return true if enabled.
	 */
	default boolean isConsumerBatchEnabled() {
		return false;
	}

	/**
	 * Set the queue names or AMQP 1.0 addresses.
	 * @param queues the queue names.
	 */
	void setQueueNames(String... queues);

	/**
	 * Set auto startup.
	 * @param autoStart true to auto start.
	 */
	void setAutoStartup(boolean autoStart);

	/**
	 * Get the message listener.
	 * @return The message listener object.
	 */
	@Nullable
	Object getMessageListener();

	/**
	 * Set the listener id.
	 * @param id the id.
	 */
	void setListenerId(String id);

	@Override
	default void afterPropertiesSet() {
	}

}
