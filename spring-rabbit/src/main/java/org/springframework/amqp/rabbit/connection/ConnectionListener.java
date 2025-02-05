/*
 * Copyright 2002-2025 the original author or authors.
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

import com.rabbitmq.client.ShutdownSignalException;
import org.jspecify.annotations.Nullable;

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
	void onCreate(@Nullable Connection connection);

	/**
	 * Called when a connection is closed.
	 * @param connection the connection.
	 * @see #onShutDown(ShutdownSignalException)
	 */
	default void onClose(Connection connection) {
	}

	/**
	 * Called when a connection is force closed.
	 * @param signal the shutdown signal.
	 * @since 2.0
	 */
	default void onShutDown(ShutdownSignalException signal) {
	}

	/**
	 * Called when a connection couldn't be established.
	 * @param exception the exception thrown.
	 * @since 2.2.17
	 */
	default void onFailed(Exception exception) {
	}

}
