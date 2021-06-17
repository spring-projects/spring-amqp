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

package org.springframework.amqp.rabbit.listener.api;

import java.util.List;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.lang.Nullable;

import com.rabbitmq.client.Channel;

/**
 * A message listener that is aware of the Channel on which the message was received.
 *
 * @author Mark Pollack
 * @author Gary Russell
 */
@FunctionalInterface
public interface ChannelAwareMessageListener extends MessageListener {

	/**
	 * Callback for processing a received Rabbit message.
	 * <p>Implementors are supposed to process the given Message,
	 * typically sending reply messages through the given Session.
	 * @param message the received AMQP message (never <code>null</code>)
	 * @param channel the underlying Rabbit Channel (never <code>null</code>
	 * unless called by the stream listener container).
	 * @throws Exception Any.
	 */
	void onMessage(Message message, @Nullable Channel channel) throws Exception; // NOSONAR

	@Override
	default void onMessage(Message message) {
		throw new IllegalStateException("Should never be called for a ChannelAwareMessageListener");
	}

	@SuppressWarnings("unused")
	default void onMessageBatch(List<Message> messages, Channel channel) {
		throw new UnsupportedOperationException("This listener does not support message batches");
	}

}
