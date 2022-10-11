/*
 * Copyright 2015-2022 the original author or authors.
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
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.io.Resource;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * A {@link RoutingConnectionFactory} that determines the node on which a queue is located and
 * returns a factory that connects directly to that node.
 * The RabbitMQ management plugin is called over REST to determine the node and the corresponding
 * address for that node is injected into the connection factory.
 * A single instance of each connection factory is retained in a cache.
 * If the location cannot be determined, the default connection factory is returned. This connection
 * factory is typically configured to connect to all the servers in a fail-over mode.
 * <p>{@link #getTargetConnectionFactory(Object)} is invoked by the
 * {@code SimpleMessageListenerContainer}, when establishing a connection, with the lookup key having
 * the format {@code '[queueName]'}.
 * <p>All {@link ConnectionFactory} methods delegate to the default
 *
 * @author Gary Russell
 * @since 1.2
 */
public class LocalizedQueueConnectionFactory implements ConnectionFactory, RoutingConnectionFactory, DisposableBean {

	private static final boolean USING_WEBFLUX;

	static {
		USING_WEBFLUX = ClassUtils.isPresent("org.springframework.web.reactive.function.client.WebClient",
				LocalizedQueueConnectionFactory.class.getClassLoader());
	}

	private final Log logger = LogFactory.getLog(getClass());

	private final Map<String, ConnectionFactory> nodeFactories = new HashMap<String, ConnectionFactory>();

	private final ConnectionFactory defaultConnectionFactory;

	private final String[] adminUris;

	private final Map<String, String> nodeToAddress = new HashMap<String, String>();

	private final String vhost;

	private final String username;

	private final String password;

	private final boolean useSSL;

	private final Resource sslPropertiesLocation;

	private final String keyStore;

	private final String trustStore;

	private final String keyStorePassPhrase;

	private final String trustStorePassPhrase;

	private NodeLocator<?> nodeLocator;

	/**
	 * @param defaultConnectionFactory the fallback connection factory to use if the queue
	 * can't be located.
	 * @param nodeToAddress a Map of node to address: (rabbit@server1 : server1:5672)
	 * @param adminUris the rabbitmq admin addresses (https://host:port, ...) must be the
	 * same length as addresses.
	 * @param vhost the virtual host.
	 * @param username the user name.
	 * @param password the password.
	 * @param useSSL use SSL.
	 * @param sslPropertiesLocation the SSL properties location.
	 */
	public LocalizedQueueConnectionFactory(ConnectionFactory defaultConnectionFactory,
			Map<String, String> nodeToAddress, String[] adminUris, String vhost, String username, String password,
			boolean useSSL, Resource sslPropertiesLocation) {

		this(defaultConnectionFactory, adminUris, nodeToAddress, vhost, username, password, useSSL,
				sslPropertiesLocation, null, null, null, null);
	}

	/**
	 * @param defaultConnectionFactory the fallback connection factory to use if the queue can't be located.
	 * @param nodeToAddress a Map of node to address: (rabbit@server1 : server1:5672)
	 * @param adminUris the rabbitmq admin addresses (https://host:port, ...) must be the same length
	 * as addresses.
	 * @param vhost the virtual host.
	 * @param username the user name.
	 * @param password the password.
	 * @param useSSL use SSL.
	 * @param keyStore the key store resource (e.g. "file:/foo/keystore").
	 * @param trustStore the trust store resource (e.g. "file:/foo/truststore").
	 * @param keyStorePassPhrase the pass phrase for the key store.
	 * @param trustStorePassPhrase the pass phrase for the trust store.
	 */
	public LocalizedQueueConnectionFactory(ConnectionFactory defaultConnectionFactory,
			Map<String, String> nodeToAddress, String[] adminUris, String vhost, String username, String password,
			boolean useSSL, String keyStore, String trustStore,
			String keyStorePassPhrase, String trustStorePassPhrase) {

		this(defaultConnectionFactory, adminUris, nodeToAddress, vhost, username, password, useSSL, null,
				keyStore, trustStore, keyStorePassPhrase, trustStorePassPhrase);
	}

	/**
	 * @param defaultConnectionFactory the fallback connection factory to use if the queue
	 * can't be located.
	 * @param addresses the rabbitmq server addresses (host:port, ...).
	 * @param adminUris the rabbitmq admin addresses (https://host:port, ...)
	 * @param nodes the rabbitmq nodes corresponding to addresses (rabbit@server1, ...)
	 * must be the same length as addresses.
	 * @param vhost the virtual host.
	 * @param username the user name.
	 * @param password the password.
	 * @param useSSL use SSL.
	 * @param sslPropertiesLocation the SSL properties location.
	 */
	public LocalizedQueueConnectionFactory(ConnectionFactory defaultConnectionFactory, String[] addresses,
			String[] adminUris, String[] nodes, String vhost, String username, String password, boolean useSSL,
			@Nullable Resource sslPropertiesLocation) {

		this(defaultConnectionFactory, adminUris, nodesAddressesToMap(nodes, addresses), vhost, username, password,
				useSSL, sslPropertiesLocation, null, null, null, null);
	}

	/**
	 * @param defaultConnectionFactory the fallback connection factory to use if the queue can't be located.
	 * @param addresses the rabbitmq server addresses (host:port, ...).
	 * @param adminUris the rabbitmq admin addresses (https://host:port, ...).
	 * @param nodes the rabbitmq nodes corresponding to addresses (rabbit@server1, ...)  must be the same length
	 * as addresses.
	 * @param vhost the virtual host.
	 * @param username the user name.
	 * @param password the password.
	 * @param useSSL use SSL.
	 * @param keyStore the key store resource (e.g. "file:/foo/keystore").
	 * @param trustStore the trust store resource (e.g. "file:/foo/truststore").
	 * @param keyStorePassPhrase the pass phrase for the key store.
	 * @param trustStorePassPhrase the pass phrase for the trust store.
	 */
	public LocalizedQueueConnectionFactory(ConnectionFactory defaultConnectionFactory,
			String[] addresses, String[] adminUris, String[] nodes, String vhost,
			String username, String password, boolean useSSL, String keyStore, String trustStore,
			String keyStorePassPhrase, String trustStorePassPhrase) {

		this(defaultConnectionFactory, adminUris, nodesAddressesToMap(nodes, addresses), vhost, username, password,
				useSSL, null, keyStore, trustStore, keyStorePassPhrase, trustStorePassPhrase);
	}

	private LocalizedQueueConnectionFactory(ConnectionFactory defaultConnectionFactory, String[] adminUris,
			Map<String, String> nodeToAddress, String vhost, String username, String password, boolean useSSL,
			@Nullable Resource sslPropertiesLocation, @Nullable String keyStore, @Nullable String trustStore,
			@Nullable String keyStorePassPhrase, @Nullable String trustStorePassPhrase) {

		Assert.notNull(defaultConnectionFactory, "'defaultConnectionFactory' cannot be null");
		this.defaultConnectionFactory = defaultConnectionFactory;
		this.adminUris = Arrays.copyOf(adminUris, adminUris.length);
		this.nodeToAddress.putAll(nodeToAddress);
		this.vhost = vhost;
		this.username = username;
		this.password = password;
		this.useSSL = useSSL;
		this.sslPropertiesLocation = sslPropertiesLocation;
		this.keyStore = keyStore;
		this.trustStore = trustStore;
		this.keyStorePassPhrase = keyStorePassPhrase;
		this.trustStorePassPhrase = trustStorePassPhrase;
		if (USING_WEBFLUX) {
			this.nodeLocator = new WebFluxNodeLocator();
		}
		else {
			this.nodeLocator = new RestTemplateNodeLocator();
		}
	}

	private static Map<String, String> nodesAddressesToMap(String[] nodes, String[] addresses) {
		Assert.isTrue(addresses.length == nodes.length,
				"'addresses' and 'nodes' properties must have equal length");
		return IntStream.range(0, addresses.length)
			.mapToObj(i -> new SimpleImmutableEntry<>(nodes[i], addresses[i]))
			.collect(Collectors.toMap(SimpleImmutableEntry::getKey, SimpleImmutableEntry::getValue));
	}

	/**
	 * Set a {@link NodeLocator} to use to find the node address for the leader.
	 * @param nodeLocator the locator.
	 * @since 2.4.8
	 */
	public void setNodeLocator(NodeLocator<?> nodeLocator) {
		Assert.notNull(nodeLocator, "'nodeLocator' cannot be null");
		this.nodeLocator = nodeLocator;
	}

	@Override
	public Connection createConnection() throws AmqpException {
		return this.defaultConnectionFactory.createConnection();
	}

	@Override
	public String getHost() {
		return this.defaultConnectionFactory.getHost();
	}

	@Override
	public int getPort() {
		return this.defaultConnectionFactory.getPort();
	}

	@Override
	public String getVirtualHost() {
		return this.vhost;
	}

	@Override
	public String getUsername() {
		return this.username;
	}

	@Override
	public void addConnectionListener(ConnectionListener listener) {
		this.defaultConnectionFactory.addConnectionListener(listener);
	}

	@Override
	public boolean removeConnectionListener(ConnectionListener listener) {
		return this.defaultConnectionFactory.removeConnectionListener(listener);
	}

	@Override
	public void clearConnectionListeners() {
		this.defaultConnectionFactory.clearConnectionListeners();
	}

	@Override
	public ConnectionFactory getTargetConnectionFactory(Object key) {
		String queue = ((String) key);
		queue = queue.substring(1, queue.length() - 1);
		Assert.isTrue(!queue.contains(","),
				() -> "Cannot use LocalizedQueueConnectionFactory with more than one queue: " + key);
		ConnectionFactory connectionFactory = determineConnectionFactory(queue);
		if (connectionFactory == null) {
			return this.defaultConnectionFactory;
		}
		else {
			return connectionFactory;
		}
	}

	@Nullable
	private ConnectionFactory determineConnectionFactory(String queue) {
		ConnectionFactory cf = this.nodeLocator.locate(this.adminUris, this.nodeToAddress, this.vhost, this.username,
				this.password, queue, this::nodeConnectionFactory);
		if (cf == null) {
			this.logger.warn("Failed to determine queue location for: " + queue + ", using default connection factory");
		}
		return cf;
	}

	private synchronized ConnectionFactory nodeConnectionFactory(String queue, String node, String address)  {
		if (this.logger.isInfoEnabled()) {
			this.logger.info("Queue: " + queue + " is on node: " + node + " at: " + address);
		}
		ConnectionFactory cf = this.nodeFactories.get(node);
		if (cf == null) {
			cf = createConnectionFactory(address, node);
			if (this.logger.isInfoEnabled()) {
				this.logger.info("Created new connection factory: " + cf);
			}
			this.nodeFactories.put(node, cf);
		}
		return cf;
	}

	/**
	 * Create a dedicated connection factory for the address.
	 * @param address the address to which the factory should connect.
	 * @param node  the node.
	 * @return the connection factory.
	 */
	protected ConnectionFactory createConnectionFactory(String address, String node) {
		RabbitConnectionFactoryBean rcfb = new RabbitConnectionFactoryBean();
		rcfb.setUseSSL(this.useSSL);
		rcfb.setSslPropertiesLocation(this.sslPropertiesLocation);
		rcfb.setKeyStore(this.keyStore);
		rcfb.setTrustStore(this.trustStore);
		rcfb.setKeyStorePassphrase(this.keyStorePassPhrase);
		rcfb.setTrustStorePassphrase(this.trustStorePassPhrase);
		rcfb.afterPropertiesSet();
		com.rabbitmq.client.ConnectionFactory rcf;
		try {
			rcf = rcfb.getObject();
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
		CachingConnectionFactory ccf = new CachingConnectionFactory(rcf); // NOSONAR never null
		ccf.setAddresses(address);
		ccf.setUsername(this.username);
		ccf.setPassword(this.password);
		ccf.setVirtualHost(this.vhost);
		ccf.setBeanName("node:" + node);
		return ccf;
	}

	@Override
	public void resetConnection() {
		Exception lastException = null;
		for (ConnectionFactory connectionFactory : this.nodeFactories.values()) {
			if (connectionFactory instanceof DisposableBean disposable) {
				try {
					disposable.destroy();
				}
				catch (Exception e) {
					lastException = e;
				}
			}
		}
		if (lastException != null) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(lastException);
		}
	}

	@Override
	public void destroy() {
		resetConnection();
	}

	/**
	 * Used to obtain a connection factory for the queue leader.
	 *
	 * @param <T> the client type.
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
							if (uri != null) {
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

	/**
	 * Callback to determine the connection factory using the provided information.
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
		ConnectionFactory locate(String queueName, String node, String nodeUri);

	}

}
