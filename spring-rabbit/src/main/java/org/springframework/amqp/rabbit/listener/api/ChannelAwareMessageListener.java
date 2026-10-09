/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.api;

import java.util.List;

import com.rabbitmq.client.Channel;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

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
	default void onMessageBatch(List<Message> messages, @Nullable Channel channel) {
		throw new UnsupportedOperationException("This listener does not support message batches");
	}

}
