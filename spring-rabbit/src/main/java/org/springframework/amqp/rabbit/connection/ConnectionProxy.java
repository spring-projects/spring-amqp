/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
