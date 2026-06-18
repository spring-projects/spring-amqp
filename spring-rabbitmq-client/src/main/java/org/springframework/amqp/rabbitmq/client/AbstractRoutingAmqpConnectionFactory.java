/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.rabbitmq.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.rabbitmq.client.amqp.Connection;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Abstract {@link AmqpConnectionFactory} implementation that routes {@link #getConnection()}
 * calls to one of various target {@link AmqpConnectionFactory AmqpConnectionFactories} based
 * on a lookup key. The latter is usually (but not necessarily) determined through some
 * thread-bound context.
 *
 * @author Robin Collard
 *
 * @since 4.1.1
 *
 * @see RoutingAmqpConnectionFactory
 */
public abstract class AbstractRoutingAmqpConnectionFactory
		implements AmqpConnectionFactory, RoutingAmqpConnectionFactory, InitializingBean {

	private final Map<Object, AmqpConnectionFactory> targetConnectionFactories = new ConcurrentHashMap<>();

	private @Nullable AmqpConnectionFactory defaultTargetConnectionFactory;

	private boolean lenientFallback = true;

	/**
	 * Specify the map of target {@link AmqpConnectionFactory AmqpConnectionFactories},
	 * with the lookup key as key.
	 * <p>The key can be of arbitrary type; this class implements the generic lookup process
	 * only. The concrete key representation will be handled by {@link #determineCurrentLookupKey()}.
	 * @param targetConnectionFactories the target connection factories and lookup keys.
	 */
	public void setTargetConnectionFactories(Map<Object, AmqpConnectionFactory> targetConnectionFactories) {
		Assert.notNull(targetConnectionFactories, "'targetConnectionFactories' must not be null.");
		Assert.noNullElements(targetConnectionFactories.values().toArray(),
				"'targetConnectionFactories' cannot have null values.");
		this.targetConnectionFactories.putAll(targetConnectionFactories);
	}

	/**
	 * Specify the default target {@link AmqpConnectionFactory}, if any.
	 * <p>This {@link AmqpConnectionFactory} will be used as target if none of the keyed
	 * {@link #targetConnectionFactories} match the {@link #determineCurrentLookupKey()}
	 * current lookup key.
	 * @param defaultTargetConnectionFactory the default target connection factory.
	 */
	public void setDefaultTargetConnectionFactory(AmqpConnectionFactory defaultTargetConnectionFactory) {
		this.defaultTargetConnectionFactory = defaultTargetConnectionFactory;
	}

	/**
	 * Specify whether to apply a lenient fallback to the default {@link AmqpConnectionFactory}
	 * if no specific {@link AmqpConnectionFactory} could be found for the current lookup key.
	 * <p>Default is "true", accepting lookup keys without a corresponding entry in the
	 * {@link #targetConnectionFactories} - simply falling back to the default
	 * {@link AmqpConnectionFactory} in that case.
	 * <p>Switch this flag to "false" if you would prefer the fallback to only apply if the
	 * lookup key was {@code null}. Lookup keys without an {@link AmqpConnectionFactory} entry
	 * will then lead to an {@link IllegalStateException}.
	 * @param lenientFallback true to fall back to the default, if specified.
	 * @see #setTargetConnectionFactories
	 * @see #setDefaultTargetConnectionFactory
	 * @see #determineCurrentLookupKey()
	 */
	public void setLenientFallback(boolean lenientFallback) {
		this.lenientFallback = lenientFallback;
	}

	public boolean isLenientFallback() {
		return this.lenientFallback;
	}

	@Override
	public void afterPropertiesSet() {
		Assert.isTrue(this.defaultTargetConnectionFactory != null || !this.targetConnectionFactories.isEmpty(),
				"At least one target factory (or default) is required");
	}

	@Override
	public Connection getConnection() {
		return determineTargetConnectionFactory().getConnection();
	}

	/**
	 * Retrieve the current target {@link AmqpConnectionFactory}. Determines the
	 * {@link #determineCurrentLookupKey() current lookup key}, performs a lookup in the
	 * {@link #targetConnectionFactories} map, falls back to the specified
	 * {@link #defaultTargetConnectionFactory} if necessary.
	 * @return the connection factory.
	 * @see #determineCurrentLookupKey()
	 */
	protected AmqpConnectionFactory determineTargetConnectionFactory() {
		Object lookupKey = determineCurrentLookupKey();
		AmqpConnectionFactory connectionFactory = null;
		if (lookupKey != null) {
			connectionFactory = this.targetConnectionFactories.get(lookupKey);
		}
		if (connectionFactory == null && (this.lenientFallback || lookupKey == null)) {
			connectionFactory = this.defaultTargetConnectionFactory;
		}
		if (connectionFactory == null) {
			throw new IllegalStateException("Cannot determine target AmqpConnectionFactory for lookup key ["
					+ lookupKey + "]");
		}
		return connectionFactory;
	}

	@Override
	public @Nullable AmqpConnectionFactory getTargetConnectionFactory(Object key) {
		return this.targetConnectionFactories.get(key);
	}

	/**
	 * Adds the given {@link AmqpConnectionFactory} and associates it with the given lookup key.
	 * @param key the lookup key.
	 * @param connectionFactory the {@link AmqpConnectionFactory}.
	 */
	protected void addTargetConnectionFactory(Object key, AmqpConnectionFactory connectionFactory) {
		this.targetConnectionFactories.put(key, connectionFactory);
	}

	/**
	 * Removes the {@link AmqpConnectionFactory} associated with the given lookup key and returns it.
	 * @param key the lookup key.
	 * @return the {@link AmqpConnectionFactory} that was removed.
	 */
	protected @Nullable AmqpConnectionFactory removeTargetConnectionFactory(Object key) {
		return this.targetConnectionFactories.remove(key);
	}

	/**
	 * Determine the current lookup key. This will typically be implemented to check a
	 * thread-bound context.
	 * @return the lookup key.
	 */
	protected abstract @Nullable Object determineCurrentLookupKey();

}
