/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.connection;

import java.util.Map;

import org.springframework.amqp.AmqpException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Abstract {@link ConnectionFactory} implementation that routes {@link #createConnection()}
 * calls to one of various target ConnectionFactories based on a lookup key. The latter is usually
 * (but not necessarily) determined through some thread-bound context.
 *
 * @author Artem Bilan
 * @since 1.3
 */
public abstract class AbstractRoutingConnectionFactory implements ConnectionFactory, InitializingBean {

	private Map<Object, ConnectionFactory> targetConnectionFactories;

	private ConnectionFactory defaultTargetConnectionFactory;

	private boolean lenientFallback = true;

	/**
	 * Specify the map of target ConnectionFactories, with the lookup key as key.
	 * <p>The key can be of arbitrary type; this class implements the
	 * generic lookup process only. The concrete key representation will
	 * be handled by {@link #determineCurrentLookupKey()}.
	 *
	 * @param targetConnectionFactories The target connection factories and lookup keys.
	 */
	public void setTargetConnectionFactories(Map<Object, ConnectionFactory> targetConnectionFactories) {
		Assert.notNull(targetConnectionFactories, "'targetConnectionFactories' must not be null.");
		Assert.noNullElements(targetConnectionFactories.values().toArray(), "'targetConnectionFactories' cannot have null values.");
		this.targetConnectionFactories = targetConnectionFactories;
	}

	/**
	 * Specify the default target {@link ConnectionFactory}, if any.
	 * <p>This {@link ConnectionFactory} will be used as target if none of the keyed
	 * {@link #targetConnectionFactories} match the
	 * {@link #determineCurrentLookupKey()} current lookup key.
	 *
	 * @param defaultTargetConnectionFactory The default target connection factory.
	 */
	public void setDefaultTargetConnectionFactory(ConnectionFactory defaultTargetConnectionFactory) {
		this.defaultTargetConnectionFactory = defaultTargetConnectionFactory;
	}

	/**
	 * Specify whether to apply a lenient fallback to the default {@link ConnectionFactory}
	 * if no specific {@link ConnectionFactory} could be found for the current lookup key.
	 * <p>Default is "true", accepting lookup keys without a corresponding entry
	 * in the {@link #targetConnectionFactories} - simply falling back to the default
	 * {@link ConnectionFactory} in that case.
	 * <p>Switch this flag to "false" if you would prefer the fallback to only apply
	 * if the lookup key was {@code null}. Lookup keys without a {@link ConnectionFactory}
	 * entry will then lead to an {@link IllegalStateException}.
	 *
	 * @param lenientFallback true to fall back to the default, if specified.
	 *
	 * @see #setTargetConnectionFactories
	 * @see #setDefaultTargetConnectionFactory
	 * @see #determineCurrentLookupKey()
	 */
	public void setLenientFallback(boolean lenientFallback) {
		this.lenientFallback = lenientFallback;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.targetConnectionFactories, "'targetConnectionFactories' must be provided.");
	}

	@Override
	public Connection createConnection() throws AmqpException {
		return this.determineTargetConnectionFactory().createConnection();
	}

	/**
	 * Retrieve the current target {@link ConnectionFactory}. Determines the
	 * {@link #determineCurrentLookupKey() current lookup key}, performs
	 * a lookup in the {@link #targetConnectionFactories} map,
	 * falls back to the specified
	 * {@link #defaultTargetConnectionFactory} if necessary.
	 *
	 * @return The connection factory.
	 *
	 * @see #determineCurrentLookupKey()
	 */
	protected ConnectionFactory determineTargetConnectionFactory() {
		Object lookupKey = this.determineCurrentLookupKey();
		ConnectionFactory connectionFactory = this.targetConnectionFactories.get(lookupKey);
		if (connectionFactory == null && (this.lenientFallback || lookupKey == null)) {
			connectionFactory = this.defaultTargetConnectionFactory;
		}
		if (connectionFactory == null) {
			throw new IllegalStateException("Cannot determine target ConnectionFactory for lookup key [" + lookupKey + "]");
		}
		return connectionFactory;
	}

	@Override
	public void addConnectionListener(ConnectionListener listener) {
		for (ConnectionFactory connectionFactory : this.targetConnectionFactories.values()) {
			connectionFactory.addConnectionListener(listener);
		}

		if (this.defaultTargetConnectionFactory != null) {
			this.defaultTargetConnectionFactory.addConnectionListener(listener);
		}
	}

	@Override
	public boolean removeConnectionListener(ConnectionListener listener) {
		boolean removed = false;
		for (ConnectionFactory connectionFactory : this.targetConnectionFactories.values()) {
			boolean listenerRemoved = connectionFactory.removeConnectionListener(listener);
			if (!removed) {
				removed = listenerRemoved;
			}
		}

		if (this.defaultTargetConnectionFactory != null) {
			boolean listenerRemoved = this.defaultTargetConnectionFactory.removeConnectionListener(listener);
			if (!removed) {
				removed = listenerRemoved;
			}
		}
		return removed;
	}

	@Override
	public void clearConnectionListeners() {
		for (ConnectionFactory connectionFactory : this.targetConnectionFactories.values()) {
			connectionFactory.clearConnectionListeners();
		}

		if (this.defaultTargetConnectionFactory != null) {
			this.defaultTargetConnectionFactory.clearConnectionListeners();
		}
	}

	@Override
	public String getHost() {
		return this.determineTargetConnectionFactory().getHost();
	}

	@Override
	public int getPort() {
		return this.determineTargetConnectionFactory().getPort();
	}

	@Override
	public String getVirtualHost() {
		return this.determineTargetConnectionFactory().getVirtualHost();
	}

	/**
	 * Determine the current lookup key. This will typically be implemented to check a thread-bound context.
	 *
	 * @return The lookup key.
	 */
	protected abstract Object determineCurrentLookupKey();

}
