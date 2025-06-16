/*
 * Copyright 2022-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.connection;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;

/**
 * Used to obtain a connection factory for the queue leader.
 * @param <T> the client type.
 *
 * @author Gary Russell
 * @since 2.4.8
 */
public interface NodeLocator<T> {

	LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(NodeLocator.class));

	/**
	 * Return a connection factory for the leader node for the queue.
	 * @param adminUris an array of admin URIs.
	 * @param nodeToAddress a map of node names to node addresses (AMQP).
	 * @param vhost the vhost.
	 * @param username the user name.
	 * @param password the password.
	 * @param queue the queue name.
	 * @param factoryFunction an internal function to find or create the factory.
	 * @return a connection factory, if the leader node was found; null otherwise.
	 */
	@Nullable
	default ConnectionFactory locate(String[] adminUris, Map<String, String> nodeToAddress, String vhost,
			String username, String password, String queue, FactoryFinder factoryFunction) {

		T client = createClient(username, password);

		for (int i = 0; i < adminUris.length; i++) {
			String adminUri = adminUris[i];
			if (!adminUri.endsWith("/api/")) {
				adminUri += "/api/";
			}
			try {
				String uri = new URI(adminUri)
						.resolve("/api/queues/").toString();
				Map<String, Object> queueInfo = restCall(client, uri, vhost, queue);
				if (queueInfo != null) {
					String node = (String) queueInfo.get("node");
					if (node != null) {
						String nodeUri = nodeToAddress.get(node);
						if (nodeUri != null) {
							close(client);
							return factoryFunction.locate(queue, node, nodeUri);
						}
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("No match for node: " + node);
						}
					}
				}
				else {
					throw new AmqpException("Admin returned null QueueInfo");
				}
			}
			catch (Exception e) {
				LOGGER.warn("Failed to determine queue location for: " + queue + " at: " +
						adminUri + ": " + e.getMessage());
			}
		}
		LOGGER.warn("Failed to determine queue location for: " + queue + ", using default connection factory");
		close(client);
		return null;
	}

	/**
	 * Create a client for subsequent use.
	 * @param userName the user name.
	 * @param password the password.
	 * @return the client.
	 */
	T createClient(String userName, String password);

	/**
	 * Close the client.
	 * @param client the client.
	 */
	default void close(T client) {
	}

	/**
	 * Retrieve a map of queue properties using the RabbitMQ Management REST API.
	 * @param client the client.
	 * @param baseUri the base uri.
	 * @param vhost the virtual host.
	 * @param queue the queue name.
	 * @return the map of queue properties.
	 * @throws URISyntaxException if the syntax is bad.
	 */
	@Nullable
	Map<String, Object> restCall(T client, String baseUri, String vhost, String queue)
			throws URISyntaxException;

}
