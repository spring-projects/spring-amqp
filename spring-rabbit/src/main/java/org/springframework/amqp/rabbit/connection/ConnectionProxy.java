/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import org.jspecify.annotations.Nullable;

/**
 * Subinterface of {@link Connection} to be implemented by
 * Connection proxies.  Allows access to the underlying target Connection
 *
 * @author Dave Syer
 * @see CachingConnectionFactory
 */
public interface ConnectionProxy extends Connection {

	/**
	 * Return the target Channel of this proxy.
	 * <p>This will typically be the native provider Connection
	 * @return the underlying Connection (if any)
	 */
	@Nullable
	Connection getTargetConnection();

}
