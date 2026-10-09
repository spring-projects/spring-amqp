/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * A listener for new channel creation and destruction.
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 */
@FunctionalInterface
public interface ChannelListener {

	/**
	 * Called when a new channel is created.
	 * @param channel the channel.
	 * @param transactional true if transactional.
	 */
	void onCreate(Channel channel, boolean transactional);

	/**
	 * Called when the underlying RabbitMQ channel is closed for any
	 * reason.
	 * @param signal the shutdown signal.
	 */
	default void onShutDown(ShutdownSignalException signal) {
	}

}
