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

import com.rabbitmq.client.ShutdownSignalException;

/**
 * A listener for connection creation and closing.
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 */
@FunctionalInterface
public interface ConnectionListener {

	/**
	 * Called when a new connection is established.
	 * @param connection the connection.
	 */
	void onCreate(Connection connection);

	/**
	 * Called when a connection is closed.
	 * @param connection the connection.
	 * @see #onShutDown(ShutdownSignalException)
	 */
	default void onClose(Connection connection) {
	}

	/**
	 * Called when a connection is force closed.
	 * @param signal the shut down signal.
	 * @since 2.0
	 */
	default void onShutDown(ShutdownSignalException signal) {
	}

}
