/*
 * Copyright 2002-present the original author or authors.
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
