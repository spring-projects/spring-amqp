/*
 * Copyright 2002-2010 the original author or authors.
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;

/**
 * A {@link ConnectionFactory} implementation that returns the same Connections from all
 * {@link #createConnection()} calls, and ignores calls to {@link com.rabbitmq.client.Connection#close()}.
 *
 * @author Mark Fisher
 * @author Mark Pollack
 */
//TODO are there heartbeats and/or exception thrown if a connection is broken?
public class SingleConnectionFactory implements ConnectionFactory, DisposableBean {

	protected final Log logger = LogFactory.getLog(getClass());

	private volatile int port = RabbitUtils.DEFAULT_PORT;

	private final com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory;

	/** Raw Rabbit Connection */
	private Connection targetConnection;

	/** Proxy Connection */
	private Connection connection;

	/** Synchronization monitor for the shared Connection */
	private final Object connectionMonitor = new Object();


	/**
	 * Create a new SingleConnectionFactory initializing the hostname to be the
	 * value returned from InetAddress.getLocalHost(), or "localhost" if getLocalHost() throws
	 * an exception.
	 */
	public SingleConnectionFactory() {
		this.rabbitConnectionFactory = new com.rabbitmq.client.ConnectionFactory();
		this.rabbitConnectionFactory.setHost(this.getDefaultHostName());
	}

	/**
	 * Create a new SingleConnectionFactory given a host name.
	 * @param hostname the host name to connect to
	 */
	public SingleConnectionFactory(String hostname) {
		Assert.hasText(hostname, "hostname is required");
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

	protected int getPort() {
		return this.port;
	}

	protected Channel getChannel(Connection connection, boolean transactional) throws IOException {
		return this.createChannel(connection);
	}

    private Channel createChannel(Connection connection) throws IOException {
        //TODO overload with channel number.
        try {
            return connection.createChannel(false);
        } catch (com.rabbitmq.client.AlreadyClosedException ace) {
            logger.info("Connection to RabbitMQ will be re-tried due to AlreadyClosedException.");
            initConnection();
            return connection.createChannel(false);
        }
    }

	public Connection createConnection() throws IOException {
		synchronized (this.connectionMonitor) {
			if (this.connection == null) {
				initConnection();
			}
			return this.connection;
		}
	}

	public void initConnection() throws IOException {
		synchronized (this.connectionMonitor) {
			if (this.targetConnection != null) {
				closeConnection(this.targetConnection);
			}
			this.targetConnection = doCreateConnection();
			prepareConnection(this.targetConnection);
			if (logger.isInfoEnabled()) {
				logger.info("Established shared Rabbit Connection: " + this.targetConnection);
			}
			this.connection = getSharedConnectionProxy(this.targetConnection);
		}
	}

	/**
	 * Close the underlying shared connection.
	 * The provider of this ConnectionFactory needs to care for proper shutdown.
	 * <p>As this bean implements DisposableBean, a bean factory will
	 * automatically invoke this on destruction of its cached singletons.
	 */
	public void destroy() {
		resetConnection();
	}

	/**
	 * Reset the underlying shared Connection, to be reinitialized on next access.
	 */
	public void resetConnection() {
		synchronized (this.connectionMonitor) {
			if (this.targetConnection != null) {
				closeConnection(this.targetConnection);
			}
			this.targetConnection = null;
			this.connection = null;
		}
	}

	/**
	 * Close the given Connection.
	 * @param connection the Connection to close
	 */
	protected void closeConnection(Connection connection) {
		if (logger.isDebugEnabled()) {
			logger.debug("Closing shared Rabbit Connection: " + this.targetConnection);
		}
		try {
			//TODO there are other close overloads close(int closeCode, java.lang.String closeMessage, int timeout)
			connection.close();
		}
		catch (Throwable ex) {
			logger.debug("Could not close shared Rabbit Connection", ex);
		}
	}

	/**
	 * Create a Rabbit Connection via this class's ConnectionFactory.
	 * @return the new Rabbit Connection
	 */
	protected Connection doCreateConnection() throws IOException {
		return new SimpleConnection(this.rabbitConnectionFactory.newConnection());
	}

	protected void prepareConnection(Connection con) throws IOException {
		//TODO configure ShutdownListener, investigate reconnection exceptions
	}

	private String getDefaultHostName() {
		String temp;
		try {
			InetAddress localMachine = InetAddress.getLocalHost();
			temp = localMachine.getHostName();
			logger.debug("Using hostname [" + temp + "] for hostname.");
		}
		catch (UnknownHostException e) {
			logger.warn("Could not get host name, using 'localhost' as default value", e);
			temp = "localhost";
		}
		return temp;
	}

	/**
	 * Wrap the given Connection with a proxy that delegates every method call to it
	 * but suppresses close calls. This is useful for allowing application code to
	 * handle a special framework Connection just like an ordinary Connection from a
	 * Rabbit ConnectionFactory.
	 * @param target the original Connection to wrap
	 * @return the wrapped Connection
	 */
	protected Connection getSharedConnectionProxy(Connection target) {
		return new SharedConnectionProxy(target);
	}

	@Override
	public String toString() {
		return "SingleConnectionFactory [host=" + rabbitConnectionFactory.getHost() + ", port=" + port + "]";
	}

	private class SharedConnectionProxy implements Connection {

		private final Connection target;

		public SharedConnectionProxy(Connection target) {
			this.target = target;
		}

		public Channel createChannel(boolean transactional) throws IOException {
			Channel channel = getChannel(this.target, transactional);
			return channel;
		}

		public void close() throws IOException {
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
			}
			else if (!target.equals(other.target))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "Shared Rabbit Connection: " + this.target;
		}

	}

}
