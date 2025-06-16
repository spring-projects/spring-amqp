/*
 * Copyright 2017-present the original author or authors.
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
