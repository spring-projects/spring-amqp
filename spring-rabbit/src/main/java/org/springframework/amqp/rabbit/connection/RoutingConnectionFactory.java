/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import org.jspecify.annotations.Nullable;

/**
 * Implementations select a connection factory based on a supplied key.
 *
 * @author Gary Russell
 * @since 1.4.5
 *
 */
@FunctionalInterface
public interface RoutingConnectionFactory {

	/**
	 * Returns the {@link ConnectionFactory} bound to given lookup key, or null if one does not exist.
	 * @param key The lookup key to which the {@link ConnectionFactory} is bound
	 * @return the {@link ConnectionFactory} bound to the given lookup key, or null if one does not exist
	 */
	@Nullable
	ConnectionFactory getTargetConnectionFactory(Object key);

}
