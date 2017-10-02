/*
 * Copyright 2002-2017 the original author or authors.
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

import java.net.URI;
import java.util.List;

import org.springframework.amqp.AmqpException;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;

/**
 * A {@link ConnectionFactory} implementation that returns the same Connections from all {@link #createConnection()}
 * calls, and ignores calls to {@link com.rabbitmq.client.Connection#close()}.
 *
 * @author Mark Fisher
 * @author Mark Pollack
 * @author Dave Syer
 * @author Steve Powell
 * @author Gary Russell
 * @author Artem Bilan
 */
public class SingleConnectionFactory extends AbstractConnectionFactory {

	/** Proxy Connection */
	private SharedConnectionProxy connection;

	/** Synchronization monitor for the shared Connection */
	private final Object connectionMonitor = new Object();

	/**
	 * Create a new SingleConnectionFactory initializing the hostname to be the value returned from
	 * InetAddress.getLocalHost(), or "localhost" if getLocalHost() throws an exception.
	 */
	public SingleConnectionFactory() {
		this((String) null);
	}

	/**
	 * Create a new SingleConnectionFactory given a host name.
	 * @param port the port to connect to
	 */
	public SingleConnectionFactory(int port) {
		this(null, port);
	}

	/**
	 * Create a new SingleConnectionFactory given a host name.
	 * @param hostname the host name to connect to
	 */
	public SingleConnectionFactory(String hostname) {
		this(hostname, com.rabbitmq.client.ConnectionFactory.DEFAULT_AMQP_PORT);
	}

	/**
	 * Create a new SingleConnectionFactory given a host name.
	 * @param hostname the host name to connect to
	 * @param port the port number to connect to
	 */
	public SingleConnectionFactory(String hostname, int port) {
		super(new com.rabbitmq.client.ConnectionFactory());
		if (!StringUtils.hasText(hostname)) {
			hostname = getDefaultHostName();
		}
		setHost(hostname);
		setPort(port);
	}

	/**
	 * Create a new SingleConnectionFactory given a {@link URI}.
	 * @param uri the amqp uri configuring the connection
	 */
	public SingleConnectionFactory(URI uri) {
		super(new com.rabbitmq.client.ConnectionFactory());
		setUri(uri);
	}

	/**
	 * Create a new SingleConnectionFactory for the given target ConnectionFactory.
	 * @param rabbitConnectionFactory the target ConnectionFactory
	 */
	public SingleConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory) {
		super(rabbitConnectionFactory);
	}

	@Override
	public void setConnectionListeners(List<? extends ConnectionListener> listeners) {
		super.setConnectionListeners(listeners);
		// If the connection is already alive we assume that the new listeners want to be notified
		if (this.connection != null) {
			this.getConnectionListener().onCreate(this.connection);
		}
	}

	@Override
	public void addConnectionListener(ConnectionListener listener) {
		super.addConnectionListener(listener);
		// If the connection is already alive we assume that the new listener wants to be notified
		if (this.connection != null) {
			listener.onCreate(this.connection);
		}
	}

	public final Connection createConnection() throws AmqpException {
		synchronized (this.connectionMonitor) {
			if (this.connection == null) {
				Connection target = doCreateConnection();
				this.connection = new SharedConnectionProxy(target);
				// invoke the listener *after* this.connection is assigned
				getConnectionListener().onCreate(target);
			}
		}
		return this.connection;
	}

	/**
	 * Close the underlying shared connection. The provider of this ConnectionFactory needs to care for proper shutdown.
	 * <p>
	 * As this bean implements DisposableBean, a bean factory will automatically invoke this on destruction of its
	 * cached singletons.
	 */
	@Override
	public final void destroy() {
		synchronized (this.connectionMonitor) {
			if (connection != null) {
				this.connection.destroy();
				this.connection = null;
			}
		}
	}

	/**
	 * Create a Connection. This implementation just delegates to the underlying Rabbit ConnectionFactory. Subclasses
	 * typically will decorate the result to provide additional features.
	 *
	 * @return the new Connection
	 */
	protected Connection doCreateConnection() {
		Connection connection = createBareConnection();
		return connection;
	}

	@Override
	public String toString() {
		return "SingleConnectionFactory [host=" + getHost() + ", port=" + getPort() + "]";
	}

	/**
	 * Wrap a raw Connection with a proxy that delegates every method call to it but suppresses close calls. This is
	 * useful for allowing application code to handle a special framework Connection just like an ordinary Connection
	 * from a Rabbit ConnectionFactory.
	 */
	private class SharedConnectionProxy implements ConnectionProxy {

		private volatile Connection target;

		SharedConnectionProxy(Connection target) {
			this.target = target;
		}

		@Override
		public Channel createChannel(boolean transactional) {
			if (!isOpen()) {
				synchronized (this) {
					if (!isOpen()) {
						logger.debug("Detected closed connection. Opening a new one before creating Channel.");
						target = createBareConnection();
						getConnectionListener().onCreate(target);
					}
				}
			}
			Channel channel = target.createChannel(transactional);
			getChannelListener().onCreate(channel, transactional);
			return channel;
		}

		@Override
		public void addBlockedListener(BlockedListener listener) {
			this.target.addBlockedListener(listener);
		}

		@Override
		public boolean removeBlockedListener(BlockedListener listener) {
			return this.target.removeBlockedListener(listener);
		}

		@Override
		public void close() {
		}

		public void destroy() {
			if (this.target != null) {
				getConnectionListener().onClose(target);
				RabbitUtils.closeConnection(this.target);
			}
			this.target = null;
		}

		@Override
		public boolean isOpen() {
			return target != null && target.isOpen();
		}

		@Override
		public Connection getTargetConnection() {
			return target;
		}

		@Override
		public int getLocalPort() {
			Connection target = this.target;
			if (target != null) {
				return target.getLocalPort();
			}
			return 0;
		}

		@Override
		public int hashCode() {
			return 31 + ((target == null) ? 0 : target.hashCode());
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			SharedConnectionProxy other = (SharedConnectionProxy) obj;
			if (target == null) {
				if (other.target != null) {
					return false;
				}
			}
			else if (!target.equals(other.target)) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return "Shared Rabbit Connection: " + this.target;
		}

	}

}
