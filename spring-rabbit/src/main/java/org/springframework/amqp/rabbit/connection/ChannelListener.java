/*
 * Copyright 2002-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
	 * @param signal the shut down signal.
	 */
	default void onShutDown(ShutdownSignalException signal) {
	}

}
