/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import com.rabbitmq.client.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

/**
 * Consumers register their primary channels with this class. This is used
 * to ensure that, when using transactions, the resource holder doesn't
 * close the primary channel being used by the Consumer.
 * This was previously in ConnectionFactoryUtils, but it caused a class
 * tangle with RabbitResourceHolder.
 *
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Norbert Wirges
 * @since 1.2
 *
 */
public final class ConsumerChannelRegistry {

	private static final Log logger = LogFactory.getLog(ConsumerChannelRegistry.class); // NOSONAR - lower case

	private static final ThreadLocal<@Nullable ChannelHolder> CONSUMER_CHANNEL = new ThreadLocal<>();

	private ConsumerChannelRegistry() {
	}

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
		CONSUMER_CHANNEL.set(new ChannelHolder(channel, connectionFactory));
	}

	/**
	 * See registerConsumerChannel. This method is called to unregister
	 * the channel when the consumer exits.
	 */
	public static void unRegisterConsumerChannel() {
		if (logger.isDebugEnabled()) {
			logger.debug("Unregistering consumer channel" + CONSUMER_CHANNEL.get());
		}
		CONSUMER_CHANNEL.remove();
	}

	/**
	 * See registerConsumerChannel. This method is called to retrieve the
	 * channel for this consumer.
	 *
	 * @return The channel.
	 */
	public static @Nullable Channel getConsumerChannel() {
		ChannelHolder channelHolder = CONSUMER_CHANNEL.get();
		return channelHolder != null
				? channelHolder.channel
				: null;
	}

	/**
	 * See registerConsumerChannel. This method is called to retrieve the
	 * channel for this consumer if the connection factory matches.
	 *
	 * @param connectionFactory The connection factory.
	 * @return The channel.
	 */
	public static @Nullable Channel getConsumerChannel(ConnectionFactory connectionFactory) {
		ChannelHolder channelHolder = CONSUMER_CHANNEL.get();
		Channel channel = null;
		if (channelHolder != null && channelHolder.connectionFactory.equals(connectionFactory)) {
			channel = channelHolder.channel;
		}
		return channel;
	}

	/**
	 * Release the claim on the consumer channel when the transaction of the listener
	 * container has completed.
	 * @since 4.2
	 * @see #claimConsumerChannel(ConnectionFactory)
	 */
	public static void releaseConsumerChannelClaim() {
		ChannelHolder channelHolder = CONSUMER_CHANNEL.get();
		if (channelHolder != null) {
			channelHolder.claimed = false;
		}
	}

	/**
	 * Return the channel for this consumer if the connection factory matches and no
	 * transaction uses it yet; the channel is then marked as used. This prevents a new
	 * transaction (for example, {@code PROPAGATION_REQUIRES_NEW}) from taking over the
	 * channel of the suspended container transaction.
	 * @param connectionFactory The connection factory.
	 * @return The channel or null.
	 */
	static @Nullable Channel claimConsumerChannel(ConnectionFactory connectionFactory) {
		ChannelHolder channelHolder = CONSUMER_CHANNEL.get();
		if (channelHolder != null && !channelHolder.claimed
				&& channelHolder.connectionFactory.equals(connectionFactory)) {

			channelHolder.claimed = true;
			return channelHolder.channel;
		}
		return null;
	}

	/**
	 * Mark the consumer channel as used if the provided channel is the consumer channel.
	 * @param channel the channel bound to a transaction.
	 */
	static void claimIfConsumerChannel(@Nullable Channel channel) {
		ChannelHolder channelHolder = CONSUMER_CHANNEL.get();
		if (channel != null && channelHolder != null && channelHolder.channel == channel) {
			channelHolder.claimed = true;
		}
	}

	private static final class ChannelHolder {

		private final Channel channel;

		private final ConnectionFactory connectionFactory;

		private boolean claimed;

		ChannelHolder(Channel channel, ConnectionFactory connectionFactory) {
			this.channel = channel;
			this.connectionFactory = connectionFactory;
		}

		@Override
		public String toString() {
			return "ChannelHolder [channel=" + this.channel + ", connectionFactory=" + this.connectionFactory
					+ ", claimed=" + this.claimed + "]";
		}

	}

}
