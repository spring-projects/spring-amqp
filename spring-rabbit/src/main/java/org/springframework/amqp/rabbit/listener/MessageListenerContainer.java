/*
 * Copyright 2014-present the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import org.springframework.amqp.core.MessageListener;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.lang.Nullable;

/**
 * Internal abstraction used by the framework representing a message
 * listener container. Not meant to be implemented externally.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @since 1.4
 */
public interface MessageListenerContainer extends SmartLifecycle, InitializingBean {

	/**
	 * Setup the message listener to use. Throws an {@link IllegalArgumentException}
	 * if that message listener type is not supported.
	 * @param messageListener the {@code object} to wrapped to the {@code MessageListener}.
	 */
	void setupMessageListener(MessageListener messageListener);

	/**
	 * Do not check for missing or mismatched queues during startup. Used for lazily
	 * loaded message listener containers to avoid a deadlock when starting such
	 * containers. Applications lazily loading containers should verify the queue
	 * configuration before loading the container bean.
	 * @since 2.1.5
	 */
	default void lazyLoad() {
		// no-op
	}

	/**
	 * Return true if this container is capable of (and configured to) create batches
	 * of consumed messages.
	 * @return true if enabled.
	 * @since 2.2.4
	 */
	default boolean isConsumerBatchEnabled() {
		return false;
	}

	/**
	 * Set the queue names.
	 * @param queues the queue names.
	 * @since 2.4
	 */
	void setQueueNames(String... queues);

	/**
	 * Set auto startup.
	 * @param autoStart true to auto start.
	 * @since 2.4
	 */
	void setAutoStartup(boolean autoStart);

	/**
	 * Get the message listener.
	 * @return The message listener object.
	 * @since 2.4
	 */
	@Nullable
	Object getMessageListener();

	/**
	 * Set the listener id.
	 * @param id the id.
	 * @since 2.4
	 */
	void setListenerId(String id);

	@Override
	default void afterPropertiesSet() {
	}

}
