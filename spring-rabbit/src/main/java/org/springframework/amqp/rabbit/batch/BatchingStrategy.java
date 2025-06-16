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

package org.springframework.amqp.rabbit.batch;

import java.util.Collection;
import java.util.Date;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Strategy for batching messages. The methods will never be called concurrently.
 * <p>
 * <b>Experimental - APIs may change.</b>
 *
 * @author Gary Russell
 * @since 1.4.1
 *
 */
public interface BatchingStrategy {

	/**
	 * Add a message to the batch and optionally release the batch.
	 * @param exchange The exchange.
	 * @param routingKey The routing key.
	 * @param message The message.
	 * @return The batched message ({@link MessageBatch}), or null if not ready to release.
	 */
	@Nullable
	MessageBatch addToBatch(@Nullable String exchange, @Nullable String routingKey, Message message);

	/**
	 * @return the date the next scheduled release should run, or null if no data to release.
	 */
	@Nullable
	Date nextRelease();

	/**
	 * Release batch(es), perhaps due to a timeout.
	 * @return The batched message(s).
	 */
	Collection<MessageBatch> releaseBatches();

	/**
	 * Return true if this strategy can decode a batch of messages from a message body.
	 * Returning true means you must override {@link #deBatch(Message, Consumer)}.
	 * @param properties the message properties.
	 * @return true if we can decode the message.
	 * @since 2.2
	 * @see #deBatch(Message, Consumer)
	 */
	default boolean canDebatch(MessageProperties properties) {
		return false;
	}

	/**
	 * Decode a message into fragments.
	 * @param message the message.
	 * @param fragmentConsumer a consumer for fragments.
	 * @since 2.2
	 * @see #canDebatch(MessageProperties)
	 */
	default void deBatch(Message message, Consumer<Message> fragmentConsumer) {
		throw new UnsupportedOperationException("Cannot debatch this message");
	}

}
