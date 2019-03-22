/*
 * Copyright 2002-2019 the original author or authors.
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

import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

/**
 * Instances of this interface support a single listener being
 * registered for publisher confirms with multiple channels,
 * by adding context to the callbacks.
 *
 * @author Gary Russell
 *
 * @since 1.0.1
 *
 */
public interface PublisherCallbackChannel extends Channel {

	/**
	 * Header used to determine which listener to invoke for a returned message.
	 */
	String RETURN_LISTENER_CORRELATION_KEY = "spring_listener_return_correlation";

	/**
	 * Header used to locate a pending confirm to which to attach a returned message.
	 */
	String RETURNED_MESSAGE_CORRELATION_KEY = "spring_returned_message_correlation";

	/**
	 * Adds a {@link Listener}.
	 * @param listener The Listener.
	 */
	void addListener(Listener listener);

	/**
	 * Expire (remove) any {@link PendingConfirm}s created before cutoffTime for the
	 * supplied listener and return them to the caller.
	 * @param listener the listener.
	 * @param cutoffTime the time before which expired messages were created.
	 * @return the list of expired confirms.
	 */
	Collection<PendingConfirm> expire(Listener listener, long cutoffTime);

	/**
	 * Get the {@link PendingConfirm}s count.
	 * @param listener the listener.
	 * @return Count of the pending confirms.
	 */
	int getPendingConfirmsCount(Listener listener);

	/**
	 * Get the total pending confirms count.
	 * @return the count.
	 * @since 2.1
	 */
	int getPendingConfirmsCount();

	/**
	 * Adds a pending confirmation to this channel's map.
	 *
	 * @param listener The listener.
	 * @param seq The key to the map.
	 * @param pendingConfirm The PendingConfirm object.
	 */
	void addPendingConfirm(Listener listener, long seq, PendingConfirm pendingConfirm);

	/**
	 * Use this to invoke methods on the underlying rabbit client {@link Channel} that
	 * are not supported by this implementation.
	 * @return The underlying rabbit client {@link Channel}.
	 * @since 1.4.
	 */
	Channel getDelegate();

	/**
	 * Set a callback to be invoked after the ack/nack has been handled.
	 * @param callback the callback.
	 * @since 2.1
	 */
	void setAfterAckCallback(Consumer<Channel> callback);

	/**
	 * Listeners implementing this interface can participate
	 * in publisher confirms received from multiple channels,
	 * by invoking addListener on each channel. Standard
	 * AMQP channels do not support a listener being
	 * registered on multiple channels.
	 */
	interface Listener {

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
		 * When called, this listener should remove all references to the
		 * channel - it will no longer be invoked by the channel.
		 *
		 * @param channel The channel.
		 */
		void revoke(Channel channel);

		/**
		 * Returns the UUID used to identify this Listener for returns.
		 * @return A string representation of the UUID.
		 */
		String getUUID();

		boolean isConfirmListener();

		boolean isReturnListener();
	}

}
