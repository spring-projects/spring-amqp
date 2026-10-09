/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import com.rabbitmq.client.Channel;
import org.jspecify.annotations.Nullable;

/**
 * Basic callback for use in RabbitTemplate.
 * @param <T> the type the callback returns.
 *
 * @author Mark Fisher
 * @author Gary Russell
 */
@FunctionalInterface
public interface ChannelCallback<T extends @Nullable Object> {

	/**
	 * Execute any number of operations against the supplied RabbitMQ
	 * {@link Channel}, possibly returning a result.
	 *
	 * @param channel The channel.
	 * @return The result.
	 * @throws Exception Not sure what else Rabbit Throws
	 */
	T doInRabbit(Channel channel) throws Exception; // NOSONAR user code might throw anything; cannot change

}
