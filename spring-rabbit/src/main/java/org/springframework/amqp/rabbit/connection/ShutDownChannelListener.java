/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
