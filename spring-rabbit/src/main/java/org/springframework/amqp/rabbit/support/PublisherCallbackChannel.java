/*
 * Copyright 2002-2012 the original author or authors.
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
package org.springframework.amqp.rabbit.support;

import java.io.IOException;
import java.util.SortedMap;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

/**
 * Instances of this interface support a single listener being
 * registered for publisher confirms with multiple channels,
 * by adding context to the callbacks.
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public interface PublisherCallbackChannel extends Channel {

	static String RETURN_CORRELATION = "spring_return_correlation";

	/**
	 * Adds a {@link Listener} and returns a reference to
	 * the pending confirms map for that listener's pending
	 * confirms, allowing the Listener to
	 * assess unconfirmed sends at any point in time.
	 * The client must <b>NOT</b> modify the contents of
	 * this array, and must synchronize on it when
	 * iterating over its collections.
	 * @param listener The Listener.
	 * @return A reference to pending confirms for the listener
	 */
	SortedMap<Long, PendingConfirm> addListener(Listener listener);

	/**
	 * Gets a reference to the current listener, or null.
	 * @return the Listener.
	 */
	boolean removeListener(Listener listener);

	/**
	 * Adds a pending confirmation to this channel's map.
	 * @param seq The key to the map.
	 * @param pendingConfirm The PendingConfirm object.
	 */
	void addPendingConfirm(Listener listener, long seq, PendingConfirm pendingConfirm);

	/**
	 * Listeners implementing this interface can participate
	 * in publisher confirms received from multiple channels,
	 * by invoking addListener on each channel. Standard
	 * AMQP channels do not support a listener being
	 * registered on multiple channels.
	 */
	public static interface Listener {

		/**
		 * Invoked by the channel when a confirm is received.
		 * @param pendingConfirm The pending confirmation, containing
		 * correlation data.
		 * @param ack true when 'ack', false when 'nack'.
		 */
		void handleConfirm(PendingConfirm pendingConfirm, boolean ack);

		void handleReturn(int replyCode,
				String replyText,
				String exchange,
				String routingKey,
				AMQP.BasicProperties properties,
				byte[] body) throws IOException;

		/**
		 * When called, this listener must remove all references to the
		 * pending confirm map.
		 * @param unconfirmed The pending confirm map.
		 */
		void removePendingConfirmsReference(Channel channel, SortedMap<Long, PendingConfirm> unconfirmed);

		/**
		 * Returns the UUID used to identify this Listener for returns.
		 * @return A string representation of the UUID.
		 */
		String getUUID();

		boolean isConfirmListener();

		boolean isReturnListener();
	}

}