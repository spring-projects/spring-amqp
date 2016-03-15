/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.connection;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.domain.QueueInfo;


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

	/**
	 * @param defaultConnectionFactory the fallback connection factory to use if the queue
	 * can't be located.
	 * @param nodeToAddress a Map of node to address: (rabbit@server1 : server1:5672)
	 * @param adminUris the rabbitmq admin addresses (http://host:port, ...) must be the
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
		Assert.notNull(defaultConnectionFactory, "'defaultConnectionFactory' cannot be null");
		this.defaultConnectionFactory = defaultConnectionFactory;
		this.adminUris = Arrays.copyOf(adminUris, adminUris.length);
		this.nodeToAddress.putAll(nodeToAddress);
		this.vhost = vhost;
		this.username = username;
		this.password = password;
		this.useSSL = useSSL;
		this.sslPropertiesLocation = sslPropertiesLocation;
		this.keyStore = this.trustStore = this.keyStorePassPhrase = this.trustStorePassPhrase = null;
	}

	/**
	 * @param defaultConnectionFactory the fallback connection factory to use if the queue can't be located.
	 * @param nodeToAddress a Map of node to address: (rabbit@server1 : server1:5672)
	 * @param adminUris the rabbitmq admin addresses (http://host:port, ...) must be the same length
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
		Assert.notNull(defaultConnectionFactory, "'defaultConnectionFactory' cannot be null");
		this.defaultConnectionFactory = defaultConnectionFactory;
		this.adminUris = Arrays.copyOf(adminUris, adminUris.length);
		this.nodeToAddress.putAll(nodeToAddress);
		this.vhost = vhost;
		this.username = username;
		this.password = password;
		this.useSSL = useSSL;
		this.sslPropertiesLocation = null;
		this.keyStore = keyStore;
		this.trustStore = trustStore;
		this.keyStorePassPhrase = keyStorePassPhrase;
		this.trustStorePassPhrase = trustStorePassPhrase;
	}

	/**
	 * @param defaultConnectionFactory the fallback connection factory to use if the queue
	 * can't be located.
	 * @param addresses the rabbitmq server addresses (host:port, ...).
	 * @param adminUris the rabbitmq admin addresses (http://host:port, ...)
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
			Resource sslPropertiesLocation) {
		Assert.notNull(defaultConnectionFactory, "'defaultConnectionFactory' cannot be null");
		Assert.isTrue(addresses.length == nodes.length,
				"'addresses', 'adminAddresses', and 'nodes' properties must have equal length");
		this.defaultConnectionFactory = defaultConnectionFactory;
		this.adminUris = Arrays.copyOf(adminUris, adminUris.length);
		for (int i = 0; i < addresses.length; i++) {
			this.nodeToAddress.put(nodes[i], addresses[i]);
		}
		this.vhost = vhost;
		this.username = username;
		this.password = password;
		this.useSSL = useSSL;
		this.sslPropertiesLocation = sslPropertiesLocation;
		this.keyStore = this.trustStore = this.keyStorePassPhrase = this.trustStorePassPhrase = null;
	}

	/**
	 * @param defaultConnectionFactory the fallback connection factory to use if the queue can't be located.
	 * @param addresses the rabbitmq server addresses (host:port, ...).
	 * @param adminUris the rabbitmq admin addresses (http://host:port, ...).
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
		Assert.notNull(defaultConnectionFactory, "'defaultConnectionFactory' cannot be null");
		Assert.isTrue(addresses.length == nodes.length,
				"'addresses', 'adminAddresses', and 'nodes' properties must have equal length");
		this.defaultConnectionFactory = defaultConnectionFactory;
		this.adminUris = Arrays.copyOf(adminUris, adminUris.length);
		for (int i = 0; i < addresses.length; i++) {
			this.nodeToAddress.put(nodes[i], addresses[i]);
		}
		this.vhost = vhost;
		this.username = username;
		this.password = password;
		this.useSSL = useSSL;
		this.sslPropertiesLocation = null;
		this.keyStore = keyStore;
		this.trustStore = trustStore;
		this.keyStorePassPhrase = keyStorePassPhrase;
		this.trustStorePassPhrase = trustStorePassPhrase;
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
		Assert.isTrue(!queue.contains(","), "Cannot use LocalizedQueueConnectionFactory with more than one queue: " + key);
		ConnectionFactory connectionFactory = determineConnectionFactory(queue);
		if (connectionFactory == null) {
			return this.defaultConnectionFactory;
		}
		else {
			return connectionFactory;
		}
	}

	private ConnectionFactory determineConnectionFactory(String queue) {
		for (int i = 0; i < this.adminUris.length; i++) {
			String adminUri = this.adminUris[i];
			if (!adminUri.endsWith("/api/")) {
				adminUri += "/api/";
			}
			try {
				Client client = createClient(adminUri, this.username, this.password);
				QueueInfo queueInfo = client.getQueue(this.vhost, queue);
				if (queueInfo != null) {
					String node = queueInfo.getNode();
					if (node != null) {
						String uri = this.nodeToAddress.get(node);
						if (uri != null) {
							return nodeConnectionFactory(queue, node, uri);
						}
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("No match for node: " + node);
						}
					}
				}
				else {
					throw new AmqpException("Admin returned null QueueInfo");
				}
			}
			catch (Exception e) {
				this.logger.warn("Failed to determine queue location for: " + queue + " at: " +
						adminUri + ": " + e.getMessage());
			}
		}
		this.logger.warn("Failed to determine queue location for: " + queue + ", using default connection factory");
		return null;
	}

	/**
	 * Create a client instance.
	 * @param adminUri the admin URI.
	 * @param username the username
	 * @param password the password.
	 * @return The client.
	 * @throws MalformedURLException if the URL is malformed
	 * @throws URISyntaxException if there is a syntax error.
	 */
	protected Client createClient(String adminUri, String username, String password) throws MalformedURLException,
			URISyntaxException {
		return new Client(adminUri, username, password);
	}

	private synchronized ConnectionFactory nodeConnectionFactory(String queue, String node, String address)
			throws Exception {
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
	 * @throws Exception if errors occur during creation.
	 */
	protected ConnectionFactory createConnectionFactory(String address, String node) throws Exception {
		RabbitConnectionFactoryBean rcfb = new RabbitConnectionFactoryBean();
		rcfb.setUseSSL(this.useSSL);
		rcfb.setSslPropertiesLocation(this.sslPropertiesLocation);
		rcfb.setKeyStore(this.keyStore);
		rcfb.setTrustStore(this.trustStore);
		rcfb.setKeyStorePassphrase(this.keyStorePassPhrase);
		rcfb.setTrustStorePassphrase(this.trustStorePassPhrase);
		rcfb.afterPropertiesSet();
		CachingConnectionFactory ccf = new CachingConnectionFactory(rcfb.getObject());
		ccf.setAddresses(address);
		ccf.setUsername(this.username);
		ccf.setPassword(this.password);
		ccf.setVirtualHost(this.vhost);
		ccf.setBeanName("node:" + node);
		return ccf;
	}

	@Override
	public void destroy() throws Exception {
		for (ConnectionFactory connectionFactory : this.nodeFactories.values()) {
			((DisposableBean) connectionFactory).destroy();
		}
	}

}
