/*
 * Copyright 2002-2020 the original author or authors.
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

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.listener.adapter.ReplyPostProcessor;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.core.task.TaskExecutor;
import org.springframework.lang.Nullable;

/**
 * Model for a Rabbit listener endpoint. Can be used against a
 * {@link org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer
 * RabbitListenerConfigurer} to register endpoints programmatically.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @since 1.4
 */
public interface RabbitListenerEndpoint {

	/**
	 * @return the id of this endpoint. The id can be further qualified
	 * when the endpoint is resolved against its actual listener
	 * container.
	 * @see RabbitListenerContainerFactory#createListenerContainer
	 */
	String getId();

	/**
	 * @return the group of this endpoint or null if not in a group.
	 * @since 1.5
	 */
	String getGroup();

	/**
	 * @return the concurrency of this endpoint.
	 * @since 2.0
	 */
	String getConcurrency();

	/**
	 * Override of the default autoStartup property.
	 * @return the autoStartup.
	 * @since 2.0
	 */
	Boolean getAutoStartup();

	/**
	 * Setup the specified message listener container with the model
	 * defined by this endpoint.
	 * <p>This endpoint must provide the requested missing option(s) of
	 * the specified container to make it usable. Usually, this is about
	 * setting the {@code queues} and the {@code messageListener} to
	 * use but an implementation may override any default setting that
	 * was already set.
	 * @param listenerContainer the listener container to configure
	 */
	void setupListenerContainer(MessageListenerContainer listenerContainer);

	/**
	 * The preferred way for a container factory to pass a message converter
	 * to the endpoint's adapter.
	 * @param converter the converter.
	 * @since 2.0.8
	 */
	default void setMessageConverter(MessageConverter converter) {
		// NOSONAR
	}

	/**
	 * Used by the container factory to check if this endpoint supports the
	 * preferred way for a container factory to pass a message converter
	 * to the endpoint's adapter. If null is returned, the factory will
	 * fall back to the legacy method of passing the converter via the
	 * container.
	 * @return the converter.
	 * @since 2.0.8
	 */
	@Nullable
	default MessageConverter getMessageConverter() {
		return null;
	}

	/**
	 * Get the task executor to use for this endpoint's listener container.
	 * Overrides any executor set on the container factory.
	 * @return the executor.
	 * @since 2.2
	 */
	@Nullable
	default TaskExecutor getTaskExecutor() {
		return null;
	}

	/**
	 * Called by the container factory with the factory's batchListener property.
	 * @param batchListener the batchListener to set.
	 * @since 2.2
	 */
	default void setBatchListener(boolean batchListener) {
		// NOSONAR empty
	}

	/**
	 * Set a {@link BatchingStrategy} to use when debatching messages.
	 * @param batchingStrategy the batching strategy.
	 * @since 2.2
	 * @see #setBatchListener(boolean)
	 */
	default void setBatchingStrategy(BatchingStrategy batchingStrategy) {
		// NOSONAR empty
	}

	/**
	 * Override the container factory's {@link AcknowledgeMode}.
	 * @return the acknowledgment mode.
	 * @since 2.2
	 */
	@Nullable
	default AcknowledgeMode getAckMode() {
		return null;
	}

	/**
	 * Return a {@link ReplyPostProcessor} to post process a reply message before it is
	 * sent.
	 * @return the post processor.
	 * @since 2.2.5
	 */
	@Nullable
	default ReplyPostProcessor getReplyPostProcessor() {
		return null;
	}

}
