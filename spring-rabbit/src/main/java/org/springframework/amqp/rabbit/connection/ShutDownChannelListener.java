/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Functional sub interface enabling a lambda for the onShutDown method.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@FunctionalInterface
public interface ShutDownChannelListener extends ChannelListener {

	@Override
	default void onCreate(Channel channel, boolean transactional) {
	}

	@Override
	void onShutDown(ShutdownSignalException signal);

}
