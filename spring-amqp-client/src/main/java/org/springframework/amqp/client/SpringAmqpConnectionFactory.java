/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.jspecify.annotations.Nullable;

/**
 * The {@link SingleAmqpConnectionFactory} extension to recover the session shared by the links of the connection.
 * <p>
 * All the links opened from a ProtonJ {@link Connection} live on its default session.
 * A peer may end that session while the connection stays open, for example, on an authorization error,
 * and ProtonJ does not replace a closed default session.
 * Every link on the session is gone then, and no new link can be opened on this connection.
 * <p>
 * The {@link Connection} returned from this factory opens links on a session it manages itself,
 * and replaces that session when it is found closed on the next attempt to open a link.
 * This class is a workaround and may be removed when ProtonJ recovers the default session itself.
 *
 * @author Tanishq Choudhary
 *
 * @since 4.2
 */
public class SpringAmqpConnectionFactory extends SingleAmqpConnectionFactory {

	private final Lock instanceLock = new ReentrantLock();

	private volatile @Nullable Connection connection;

	/**
	 * Create an instance based on a {@link Client#create() default client}.
	 */
	public SpringAmqpConnectionFactory() {
	}

	/**
	 * Create an instance based on the provided {@link Client}.
	 * @param protonClient the client to use.
	 */
	public SpringAmqpConnectionFactory(Client protonClient) {
		super(protonClient);
	}

	@Override
	public Connection getConnection() {
		Connection connectionToReturn = this.connection;
		if (connectionToReturn == null) {
			this.instanceLock.lock();
			try {
				connectionToReturn = this.connection;
				if (connectionToReturn == null) {
					connectionToReturn = new SessionRecoveringConnection(super.getConnection());
					this.connection = connectionToReturn;
				}
			}
			finally {
				this.instanceLock.unlock();
			}
		}
		return connectionToReturn;
	}

	@Override
	public void destroy() {
		this.connection = null;
		super.destroy();
	}

}
