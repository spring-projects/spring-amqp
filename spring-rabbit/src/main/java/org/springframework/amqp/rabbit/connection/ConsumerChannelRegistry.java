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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rabbitmq.client.Channel;

/**
 * Consumers register their primary channels with this class. This is used
 * to ensure that, when using transactions, the resource holder doesn't
 * close the primary channel being used by the Consumer.
 * This was previously in ConnectionFactoryUtils, but it caused a class
 * tangle with RabbitResourceHolder.
 *
 * @author Gary Russell
 * @since 1.2
 *
 */
public class ConsumerChannelRegistry {

	private static final Log logger = LogFactory.getLog(ConsumerChannelRegistry.class);

	private static final ThreadLocal<ChannelHolder> consumerChannel = new ThreadLocal<ChannelHolder>();

	/**
	 * If a listener container is configured to use a RabbitTransactionManager, the
	 * consumer's channel is registered here so that it is used as the bound resource
	 * when the transaction actually starts. It is normally not necessary to use
	 * an external transaction manager because local transactions work the same in that
	 * the channel is bound to the thread. This is for the case when a user happens
	 * to wire in a RabbitTransactionManager.
	 *
	 * @param channel The channel to register.
	 * @param connectionFactory The connection factory.
	 */
	public static void registerConsumerChannel(Channel channel, ConnectionFactory connectionFactory) {
		if (logger.isDebugEnabled()) {
			logger.debug("Registering consumer channel" + channel + " from factory " +
							connectionFactory);
		}
		consumerChannel.set(new ChannelHolder(channel, connectionFactory));
	}

	/**
	 * See registerConsumerChannel. This method is called to unregister
	 * the channel when the consumer exits.
	 */
	public static void unRegisterConsumerChannel() {
		if (logger.isDebugEnabled()) {
			logger.debug("Unregistering consumer channel" + consumerChannel.get());
		}
		consumerChannel.remove();
	}

	/**
	 * See registerConsumerChannel. This method is called to retrieve the
	 * channel for this consumer.
	 *
	 * @return The channel.
	 */
	public static Channel getConsumerChannel() {
		ChannelHolder channelHolder = consumerChannel.get();
		Channel channel = null;
		if (channelHolder != null) {
			channel = channelHolder.getChannel();
		}
		return channel;
	}

	/**
	 * See registerConsumerChannel. This method is called to retrieve the
	 * channel for this consumer if the connection factory matches.
	 *
	 * @param connectionFactory The connection factory.
	 * @return The channel.
	 */
	public static Channel getConsumerChannel(ConnectionFactory connectionFactory) {
		ChannelHolder channelHolder = consumerChannel.get();
		Channel channel = null;
		if (channelHolder != null && channelHolder.getConnectionFactory() == connectionFactory) {
			channel = channelHolder.getChannel();
		}
		return channel;
	}

	private static class ChannelHolder {

		private final Channel channel;

		private final ConnectionFactory connectionFactory;

		private ChannelHolder(Channel channel, ConnectionFactory connectionFactory) {
			this.channel = channel;
			this.connectionFactory = connectionFactory;
		}

		private Channel getChannel() {
			return this.channel;
		}

		private ConnectionFactory getConnectionFactory() {
			return this.connectionFactory;
		}
	}
}
