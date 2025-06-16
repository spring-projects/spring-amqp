/*
 * Copyright 2002-present the original author or authors.
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

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;

import org.springframework.amqp.AmqpException;
import org.springframework.lang.Nullable;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
public interface Connection extends AutoCloseable {

	/**
	 * Create a new channel, using an internally allocated channel number.
	 * @param transactional true if the channel should support transactions
	 * @return a new channel descriptor, or null if none is available
	 * @throws AmqpException if an I/O problem is encountered
	 */
	Channel createChannel(boolean transactional) throws AmqpException;

	/**
	 * Close this connection and all its channels
	 * with the {@link com.rabbitmq.client.AMQP#REPLY_SUCCESS} close code
	 * and message 'OK'.
	 *
	 * Waits for all the close operations to complete.
	 *
	 * @throws AmqpException if an I/O problem is encountered
	 */
	@Override
	void close() throws AmqpException;

	/**
	 * Flag to indicate the status of the connection.
	 *
	 * @return true if the connection is open
	 */
	boolean isOpen();

	/**
	 * @return the local port if the underlying connection supports it.
	 */
	int getLocalPort();


	/**
	 * Add a {@link BlockedListener}.
	 * @param listener the listener to add
	 * @since 2.0
	 * @see com.rabbitmq.client.Connection#addBlockedListener(BlockedListener)
	 */
	void addBlockedListener(BlockedListener listener);

	/**
	 * Remove a {@link BlockedListener}.
	 * @param listener the listener to remove
	 * @return <code><b>true</b></code> if the listener was found and removed,
	 * <code><b>false</b></code> otherwise
	 * @since 2.0
	 * @see com.rabbitmq.client.Connection#removeBlockedListener(BlockedListener)
	 */
	boolean removeBlockedListener(BlockedListener listener);

	/**
	 * Return the underlying RabbitMQ connection.
	 * @return the connection.
	 */
	default @Nullable com.rabbitmq.client.Connection getDelegate() {
		return null;
	}

	/**
	 * Close any channel associated with the current thread.
	 */
	default void closeThreadChannel() {
	}

}
