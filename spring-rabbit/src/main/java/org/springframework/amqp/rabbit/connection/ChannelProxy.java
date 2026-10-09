/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import com.rabbitmq.client.Channel;

import org.springframework.aop.RawTargetAccess;

/**
 * Subinterface of {@link com.rabbitmq.client.Channel} to be implemented by
 * Channel proxies.  Allows access to the underlying target Channel
 *
 * @author Mark Pollack
 * @author Gary Russell
 * @author Leonardo Ferreira
 * @see CachingConnectionFactory
 */
public interface ChannelProxy extends Channel, RawTargetAccess {

	/**
	 * Return the target Channel of this proxy.
	 * <p>This will typically be the native provider Channel
	 * @return the underlying Channel (never <code>null</code>)
	 */
	Channel getTargetChannel();

	/**
	 * Return whether this channel has transactions enabled {@code txSelect()}.
	 * @return true if the channel is transactional.
	 * @since 1.5
	 */
	boolean isTransactional();

	/**
	 * Return true if confirms are selected on this channel.
	 * @return true if {@code confirms} selected.
	 * @since 2.1
	 */
	default boolean isConfirmSelected() {
		return false;
	}

	/**
	 * Return true if publisher confirms are enabled.
	 * @return true if publisherConfirms.
	 */
	default boolean isPublisherConfirms() {
		return false;
	}

}
