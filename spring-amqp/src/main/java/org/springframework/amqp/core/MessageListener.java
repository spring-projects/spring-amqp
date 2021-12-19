/*
 * Copyright 2002-2021 the original author or authors.
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

package org.springframework.amqp.core;

import java.util.List;

/**
 * Listener interface to receive asynchronous delivery of Amqp Messages.
 *
 * @author Mark Pollack
 * @author Gary Russell
 */
@FunctionalInterface
public interface MessageListener {

	/**
	 * Delivers a single message.
	 * @param message the message.
	 */
	void onMessage(Message message);

	/**
	 * Called by the container to inform the listener of its acknowledgement
	 * mode.
	 * @param mode the {@link AcknowledgeMode}.
	 * @since 2.1.4
	 */
	default void containerAckMode(AcknowledgeMode mode) {
		// NOSONAR - empty
	}

	/**
	 * Return true if this listener is request/reply and the replies are
	 * async.
	 * @return true for async replies.
	 * @since 2.2.21
	 */
	default boolean isAsyncReplies() {
		return false;
	}

	/**
	 * Delivers a batch of messages.
	 * @param messages the messages.
	 * @since 2.2
	 */
	default void onMessageBatch(List<Message> messages) {
		throw new UnsupportedOperationException("This listener does not support message batches");
	}

}
