/*
 * Copyright 2017 the original author or authors.
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

import org.springframework.amqp.event.AmqpEvent;

/**
 * The {@link AmqpEvent} emitted by the {@code CachingConnectionFactory}
 * when its connections are unblocked.
 *
 * @author Artem Bilan
 *
 * @since 2.0
 *
 * @see com.rabbitmq.client.BlockedListener#handleUnblocked()
 */
@SuppressWarnings("serial")
public class ConnectionUnblockedEvent extends AmqpEvent {

	public ConnectionUnblockedEvent(Connection source) {
		super(source);
	}

	public Connection getConnection() {
		return (Connection) getSource();
	}

}
