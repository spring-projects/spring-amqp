/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import java.util.concurrent.ExecutorService;

import com.rabbitmq.client.Channel;

/**
 * A factory for {@link PublisherCallbackChannel}s.
 *
 * @author Gary Russell
 * @since 2.1.6
 *
 */
@FunctionalInterface
public interface PublisherCallbackChannelFactory {

	/**
	 * Create a {@link PublisherCallbackChannel} instance based on the provided delegate
	 * and executor.
	 * @param delegate the delegate channel.
	 * @param executor the executor.
	 * @return the channel.
	 */
	PublisherCallbackChannel createChannel(Channel delegate, ExecutorService executor);

}
