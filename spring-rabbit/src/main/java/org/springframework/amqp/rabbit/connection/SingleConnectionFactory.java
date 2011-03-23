/*
 * Copyright 2002-2010 the original author or authors.
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Channel;

/**
 * A {@link ConnectionFactory} implementation that returns the same Connections from all {@link #createConnection()}
 * calls, and ignores calls to {@link com.rabbitmq.client.Connection#close()}.
 * 
 * @author Mark Fisher
 * @author Mark Pollack
 * @author Dave Syer
 */
public class SingleConnectionFactory implements ConnectionFactory, DisposableBean {

	private final Log logger = LogFactory.getLog(getClass());

	private final com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory;

	/** Raw Rabbit Connection */
	private Connection targetConnection;

	/** Proxy Connection */
	private Connection connection;

	/** Synchronization monitor for the shared Connection */
	private final Object connectionMonitor = new Object();
	
	private final CompositeConnectionListener listener = new CompositeConnectionListener();

	/**
	 * Create a new SingleConnectionFactory initializing the hostname to be the value returned from
	 * InetAddress.getLocalHost(), or "localhost" if getLocalHost() throws an exception.
	 */
	public SingleConnectionFactory() {
		this((String) null);
	}

	/**
	 * Create a new SingleConnectionFactory given a host name.
	 * @param hostname the host name to connect to
	 */
	public SingleConnectionFactory(String hostname) {
		if (!StringUtils.hasText(hostname)) {
			hostname = getDefaultHostName();
		}
		this.rabbitConnectionFactory = new com.rabbitmq.client.ConnectionFactory();
		this.rabbitConnectionFactory.setHost(hostname);
	}

	/**
	 * Create a new SingleConnectionFactory for the given target ConnectionFactory.
	 * @param rabbitConnectionFactory the target ConnectionFactory
	 */
	public SingleConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory) {
		Assert.notNull(rabbitConnectionFactory, "Target ConnectionFactory must not be null");
		this.rabbitConnectionFactory = rabbitConnectionFactory;
	}

	public void setUsername(String username) {
		this.rabbitConnectionFactory.setUsername(username);
	}

	public void setPassword(String password) {
		this.rabbitConnectionFactory.setPassword(password);
	}

	public void setHost(String host) {
		this.rabbitConnectionFactory.setHost(host);
	}

	public String getHost() {
		return this.rabbitConnectionFactory.getHost();
	}

	public void setVirtualHost(String virtualHost) {
		this.rabbitConnectionFactory.setVirtualHost(virtualHost);
	}

	public String getVirtualHost() {
		return rabbitConnectionFactory.getVirtualHost();
	}

	public void setPort(int port) {
		this.rabbitConnectionFactory.setPort(port);
	}

	public int getPort() {
		return this.rabbitConnectionFactory.getPort();
	}

	public void setConnectionListeners(List<? extends ConnectionListener> listeners) {
		this.listener.setDelegates(listeners);
	}

	public void addConnectionListener(ConnectionListener listener) {
		this.listener.addDelegate(listener);
	}

	public final Connection createConnection() throws AmqpException {
		synchronized (this.connectionMonitor) {
			if (this.connection == null) {
				if (this.targetConnection != null) {
					RabbitUtils.closeConnection(this.targetConnection);
				}
				this.targetConnection = doCreateConnection();
				if (logger.isInfoEnabled()) {
					logger.info("Established shared Rabbit Connection: " + this.targetConnection);
				}
				this.connection = new SharedConnectionProxy(this.targetConnection);
			}
			this.listener.onCreate(connection);
		}
		return this.connection;
	}

	/**
	 * Close the underlying shared connection. The provider of this ConnectionFactory needs to care for proper shutdown.
	 * <p>
	 * As this bean implements DisposableBean, a bean factory will automatically invoke this on destruction of its
	 * cached singletons.
	 */
	public final void destroy() {
		synchronized (this.connectionMonitor) {
			if (this.targetConnection != null) {
				RabbitUtils.closeConnection(this.targetConnection);
			}
			this.targetConnection = null;
			this.connection = null;
		}
		reset();
	}

	/**
	 * Default implementation does nothing. Called on {@link #destroy()}.
	 */
	protected void reset() {
	}

	/**
	 * Create a Connection. This implementation just delegates to the underlying Rabbit ConnectionFactory. Subclasses
	 * typically will decorate the result to provide additional features.
	 * 
	 * @return the new Connection
	 */
	protected Connection doCreateConnection() {
		return createBareConnection();
	}

	private Connection createBareConnection() {
		try {
			return new SimpleConnection(this.rabbitConnectionFactory.newConnection());
		} catch (IOException e) {
			throw RabbitUtils.convertRabbitAccessException(e);
		}
	}

	private String getDefaultHostName() {
		String temp;
		try {
			InetAddress localMachine = InetAddress.getLocalHost();
			temp = localMachine.getHostName();
			logger.debug("Using hostname [" + temp + "] for hostname.");
		} catch (UnknownHostException e) {
			logger.warn("Could not get host name, using 'localhost' as default value", e);
			temp = "localhost";
		}
		return temp;
	}

	@Override
	public String toString() {
		return "SingleConnectionFactory [host=" + rabbitConnectionFactory.getHost() + ", port="
				+ rabbitConnectionFactory.getPort() + "]";
	}

	/**
	 * Wrap a raw Connection with a proxy that delegates every method call to it but suppresses close calls. This is
	 * useful for allowing application code to handle a special framework Connection just like an ordinary Connection
	 * from a Rabbit ConnectionFactory.
	 */
	private class SharedConnectionProxy implements Connection, ConnectionProxy {

		private volatile Connection target;

		public SharedConnectionProxy(Connection target) {
			this.target = target;
		}

		public Channel createChannel(boolean transactional) {
			if (!target.isOpen()) {
				synchronized (this) {
					if (!target.isOpen()) {
						logger.debug("Detected closed connection. Opening a new one before creating Channel.");
						target = createBareConnection();
					}
				}
			}
			Channel channel = target.createChannel(transactional);
			return channel;
		}

		public void close() {
		}

		public boolean isOpen() {
			return target != null && target.isOpen();
		}

		public Connection getTargetConnection() {
			return target;
		}

		@Override
		public int hashCode() {
			return 31 + ((target == null) ? 0 : target.hashCode());
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			SharedConnectionProxy other = (SharedConnectionProxy) obj;
			if (target == null) {
				if (other.target != null)
					return false;
			} else if (!target.equals(other.target))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "Shared Rabbit Connection: " + this.target;
		}

	}

}
