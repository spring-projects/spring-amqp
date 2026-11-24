/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbitmq.client;

import org.jspecify.annotations.Nullable;

/**
 * Implementations select an {@link AmqpConnectionFactory} based on a supplied key.
 *
 * @author Robin Collard
 *
 * @since 4.2
 */
@FunctionalInterface
public interface RoutingAmqpConnectionFactory {

	/**
	 * Return the {@link AmqpConnectionFactory} bound to given lookup key, or {@code null}
	 * if one does not exist.
	 * @param key the lookup key to which the {@link AmqpConnectionFactory} is bound.
	 * @return the {@link AmqpConnectionFactory} bound to the given lookup key,
	 * or {@code null} if one does not exist.
	 */
	@Nullable
	AmqpConnectionFactory getTargetConnectionFactory(Object key);

}
