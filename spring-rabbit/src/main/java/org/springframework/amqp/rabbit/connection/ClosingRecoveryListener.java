/*
 * Copyright 2018-2019 the original author or authors.
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;

/**
 * A {@link RecoveryListener} that closes the recovered channel, to avoid
 * orphaned consumers.
 *
 * @author Gary Russell
 * @since 1.7.10
 *
 */
public final class ClosingRecoveryListener implements RecoveryListener {

	private static final Log logger = LogFactory.getLog(ClosingRecoveryListener.class); // NOSONAR - lower case

	private static final RecoveryListener INSTANCE = new ClosingRecoveryListener();

	private static final ConcurrentMap<AutorecoveringChannel, Boolean> hasListener // NOSONAR - lower case
		= new ConcurrentHashMap<>();

	private ClosingRecoveryListener() {
	}

	@Override
	public void handleRecovery(Recoverable recoverable) {
		// should never get here
		handleRecoveryStarted(recoverable);
	}

	@Override
	public void handleRecoveryStarted(Recoverable recoverable) {
		if (logger.isDebugEnabled()) {
			logger.debug("Closing an autorecovered channel: " + recoverable);
		}
		try {
			((Channel) recoverable).close();
		}
		catch (IOException | TimeoutException e) {
			logger.error("Error closing an autorecovered channel", e);
		}
		finally {
			hasListener.remove(recoverable);
		}
	}

	/**
	 * Add a listener if necessary so we can immediately close an autorecovered
	 * channel if necessary since the actual consumer will no longer exist.
	 * Idempotent operation.
	 * @param channel the channel.
	 */
	public static void addRecoveryListenerIfNecessary(Channel channel) {
		AutorecoveringChannel autorecoveringChannel = null;
		if (channel instanceof ChannelProxy) {
			if (((ChannelProxy) channel).getTargetChannel() instanceof AutorecoveringChannel) {
				autorecoveringChannel = (AutorecoveringChannel) ((ChannelProxy) channel)
						.getTargetChannel();
			}
		}
		else if (channel instanceof AutorecoveringChannel) {
			autorecoveringChannel = (AutorecoveringChannel) channel;
		}
		if (autorecoveringChannel != null
				&& hasListener.putIfAbsent(autorecoveringChannel, Boolean.TRUE) == null) {
			autorecoveringChannel.addRecoveryListener(INSTANCE);
		}
	}

	/**
	 * Remove the channel from the set used to ensure that
	 * {@link #addRecoveryListenerIfNecessary(Channel)} is idempotent.
	 * @param channel the channel to remove.
	 */
	public static void removeChannel(AutorecoveringChannel channel) {
		hasListener.remove(channel);
	}

}
