/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.connection.SimpleResourceHolder;

/**
 * An {@link AbstractRoutingAmqpConnectionFactory} implementation which gets a {@code lookupKey}
 * for the current {@link AmqpConnectionFactory} from a thread-bound resource by the key of the
 * instance of this {@link AmqpConnectionFactory}.
 *
 * @author Robin Collard
 *
 * @since 4.2
 *
 * @see SimpleResourceHolder
 */
public class SimpleRoutingAmqpConnectionFactory extends AbstractRoutingAmqpConnectionFactory {

	@Override
	protected @Nullable Object determineCurrentLookupKey() {
		return SimpleResourceHolder.get(this);
	}

}
