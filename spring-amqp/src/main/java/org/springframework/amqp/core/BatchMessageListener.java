/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.List;

/**
 * Used to receive a batch of messages if the container supports it.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public interface BatchMessageListener extends MessageListener {

	@Override
	default void onMessage(Message message) {
		throw new UnsupportedOperationException("Should never be called by the container");
	}

	@Override
	void onMessageBatch(List<Message> messages);


}
