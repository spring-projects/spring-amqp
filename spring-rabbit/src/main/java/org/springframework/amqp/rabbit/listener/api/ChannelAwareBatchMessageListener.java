/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.api;

import java.util.List;

import com.rabbitmq.client.Channel;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;

/**
 * Used to receive a batch of messages if the container supports it.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public interface ChannelAwareBatchMessageListener extends ChannelAwareMessageListener {

	@Override
	default void onMessage(Message message, @Nullable Channel channel) throws Exception {
		throw new UnsupportedOperationException("Should never be called by the container");
	}

	@Override
	void onMessageBatch(List<Message> messages, @Nullable Channel channel);

}
