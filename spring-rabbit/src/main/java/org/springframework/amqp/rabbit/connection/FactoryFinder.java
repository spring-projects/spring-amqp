/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
