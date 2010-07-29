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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

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

	public String getVirtualHost() {
		return rabbitConnectionFactory.getVirtualHost();
	}

	public void setPort(int port) {
		this.rabbitConnectionFactory.setPort(port);
	}

	protected int getPort() {
		return this.port;
	}

	protected Channel getChannel(Connection connection) throws Exception {
		return this.createChannel(connection);
	}

	private Channel createChannel(Connection connection) throws IOException {
		//TODO overload with channel number.
		return connection.createChannel();
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
		return this.rabbitConnectionFactory.newConnection();	
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
		List<Class<?>> classes = new ArrayList<Class<?>>(1);
		classes.add(Connection.class);		
		return (Connection) Proxy.newProxyInstance(
				Connection.class.getClassLoader(),
				classes.toArray(new Class[classes.size()]),
				new SharedConnectionInvocationHandler(target));
	}

	@Override
	public String toString() {
		return "SingleConnectionFactory [host=" + rabbitConnectionFactory.getHost() + ", port=" + port + "]";
	}


	/**
	 * Invocation handler for a cached Rabbit Connection proxy.
	 */
	private class SharedConnectionInvocationHandler implements InvocationHandler {

		private final Connection target;

		public SharedConnectionInvocationHandler(Connection target) {
			this.target = target;
		}

		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			if (method.getName().equals("equals")) {
				// Only consider equal when proxies are identical.
				return (proxy == args[0]);
			}
			else if (method.getName().equals("hashCode")) {
				// Use hashCode of Connection proxy.
				return System.identityHashCode(proxy);
			}
			else if (method.getName().equals("toString")) {
				return "Shared Rabbit Connection: " + this.target;
			}
			//TODO investigate calling addShutdownListener
			//TODO investigate calling of abort						
			else if (method.getName().equals("close")) {
				// Handle close method: don't pass the call on.
				return null;
			}
			else if (method.getName().equals("createChannel")) {
				Integer channelNumber = null;
				if (args != null && args.length == 1) { 
					channelNumber = (Integer) args[0];
				}
				//TODO take into account channel number argument
				if (channelNumber != null) {
					logger.warn("Ignoring channel number argument when creating channel.  To be fixed in the future.");
				}
				Channel channel = getChannel(this.target);
				if (channel != null) {					
					return channel;
				}
			}
			try {
				Object retVal = method.invoke(this.target, args);
				//TODO investigate exception listeners
				return retVal;				
			}
			catch (InvocationTargetException ex) {
				throw ex.getTargetException();
			}
		}
	}

}
