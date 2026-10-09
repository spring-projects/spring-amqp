/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import org.jspecify.annotations.Nullable;

/**
 * Callback to determine the connection factory using the provided information.
 *
 * @author Gary Russell
 * @since 2.4.8
 */
@FunctionalInterface
public interface FactoryFinder {

	/**
	 * Locate or create a factory.
	 * @param queueName the queue name.
	 * @param node the node name.
	 * @param nodeUri the node URI.
	 * @return the factory.
	 */
	@Nullable
	ConnectionFactory locate(@Nullable String queueName, String node, String nodeUri);

}
