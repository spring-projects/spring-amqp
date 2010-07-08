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
import java.util.LinkedList;
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
 * {@link #createConnection()} calls, and ignores calls to {@link com.rabbitmq.client.Connection#close()} and
 * caches {@link com.rabbitmq.client.Channel}.
 * 
 * <p>By default, only one single Session will be cached, with further requested Channels being created and
 * disposed on demand.  Consider raising the {@link #setChannelCacheSize(int) "channelCacheSize" value} in case of a 
 * high-concurrency environment.
 * 
 * <p><b>NOTE: This ConnectionFactory requires explicit closing of all Channels obtained form its
 * shared Connection.</b>  This is the usual recommendation for native Rabbit access code anyway.
 * However, with this ConnectionFactory, its use is mandatory in order to actually allow for Channel reuse.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
//TODO are there heartbeats and/or exception thrown if a connection is broken?
public class CachingConnectionFactory implements ConnectionFactory, DisposableBean {

	protected final Log logger = LogFactory.getLog(getClass());

	private final String hostName;

	private volatile int portNumber = RabbitUtils.DEFAULT_PORT;

	private int channelCacheSize = 1;

	private final com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory;
	
	/** Raw Rabbit Connection */
	private Connection targetConnection;

	/** Proxy Connection */
	private Connection connection;
	
	/** Synchronization monitor for the shared Connection */
	private final Object connectionMonitor = new Object();

	private final LinkedList<Channel> cachedChannels = new LinkedList<Channel>();
	
	private volatile boolean active = true;


	/**
	 * Create a new CachingConnectionFactory initializing the hostname to be the 
	 * value returned from InetAddress.getLocalHost(), or "localhost" if getLocalHost() throws
	 * an exception.
	 */
	public CachingConnectionFactory() {
		this.hostName = initializeDefaultHostName();
		this.rabbitConnectionFactory = new com.rabbitmq.client.ConnectionFactory();		
	}

	/**
	 * Create a new CachingConnectionFactory given a host name.
	 * @param hostName the host name to connect to
	 */
	public CachingConnectionFactory(String hostName) {
		this(new com.rabbitmq.client.ConnectionFactory(), hostName);
	}

	/**
	 * Create a new CachingConnectionFactory for the given target
	 * ConnectionFactory.
	 * @param rabbitConnectionFactory the target ConnectionFactory
	 * @param hostName the host name to connect to
	 */
	public CachingConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory, String hostName) {
		Assert.notNull(rabbitConnectionFactory, "Target ConnectionFactory must not be null");
		Assert.hasText(hostName, "hostName must not be empty");
		this.rabbitConnectionFactory = rabbitConnectionFactory;
		this.hostName = hostName;
	} 


	public void setUsername(String username) {
		this.rabbitConnectionFactory.setUsername(username);
	}

	public void setPassword(String password) {
		this.rabbitConnectionFactory.setPassword(password);
	}

	public void setChannelCacheSize(int sessionCacheSize) {
		Assert.isTrue(sessionCacheSize >= 1, "Channel cache size must be 1 or higher");
		this.channelCacheSize = sessionCacheSize;
	}

	public int getChannelCacheSize() {
		return this.channelCacheSize;
	}

	public String getHostName() {
		return hostName;
	}
	
	public String getVirtualHost() {
		return rabbitConnectionFactory.getVirtualHost();
	}

	protected Channel getChannel(Connection connection) throws Exception {
		LinkedList<Channel> channelList = this.cachedChannels;
		Channel channel = null;
		synchronized (channelList) {
			if (!channelList.isEmpty()) {
				channel = channelList.removeFirst();
			}
		}
		if (channel != null) {
			if (logger.isTraceEnabled()) {
				logger.trace("Found cached Rabbit Channel");
			}
		}
		else {
			Channel targetChannel = createChannel(connection);
			if (logger.isDebugEnabled()) {
				logger.debug("Creating cached Rabbit Channel");
			}
			channel = getCachedChannelProxy(targetChannel, channelList);
		}
		return channel;
	}

	private Channel createChannel(Connection connection) throws IOException {
		//TODO overload with channel number.
		return connection.createChannel();
	}

	protected Channel getCachedChannelProxy(Channel target, LinkedList<Channel> channelList) {
		return (Channel) Proxy.newProxyInstance(
				ChannelProxy.class.getClassLoader(),
				new Class[] {ChannelProxy.class},
				new CachedChannelInvocationHandler(target, channelList));
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
	 * Reset the Channel cache and underlying shared Connection, to be reinitialized on next access.
	 */
	public void resetConnection() {
		this.active = false;
		synchronized (this.cachedChannels) {
			for (Channel channel : cachedChannels) {
				try {
					channel.close();
				}
				catch (Throwable ex) {
					logger.trace("Could not close cached Rabbit Channel", ex);
				}
			}						
			this.cachedChannels.clear();
		}
		this.active = true;
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

	protected String initializeDefaultHostName() {
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
		return "CachingConnectionFactory [channelCacheSize=" + channelCacheSize
				+ ", hostName=" + hostName + ", portNumber=" + portNumber
				+ ", active=" + active + "]";
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


	public class CachedChannelInvocationHandler implements InvocationHandler {

		private final Channel target;

		private final LinkedList<Channel> channelList;

		
		public CachedChannelInvocationHandler(Channel target, LinkedList<Channel> channelList) {
			this.target = target;
			this.channelList = channelList;
		}

		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String methodName = method.getName();
			if (methodName.equals("equals")) {
				// Only consider equal when proxies are identical.
				return (proxy == args[0]);
			}
			else if (methodName.equals("hashCode")) {
				// Use hashCode of Channel proxy.
				return System.identityHashCode(proxy);
			}
			else if (methodName.equals("toString")) {
				return "Cached Rabbit Channel: " + this.target;
			}
			else if (methodName.equals("close")) {
				// Handle close method: don't pass the call on.
				if (active) {
					synchronized (this.channelList) {
						if (this.channelList.size() < getChannelCacheSize()) {
							logicalClose((Channel) proxy);
							// Remain open in the channel list.
							return null;
						}
					}
				}
				
				// If we get here, we're supposed to shut down.
				physicalClose();
				return null;
			}
			else if (methodName.equals("getTargetChannel")) {
				// Handle getTargetSession method: return underlying Channel.
				return this.target;
			}			
			try {
				return method.invoke(this.target, args);
			}
			catch (InvocationTargetException ex) {
				throw ex.getTargetException();
			}
		}

		private void logicalClose(Channel proxy) throws Exception {
			// Allow for multiple close calls...
			if (!this.channelList.contains(proxy)) {
				if (logger.isTraceEnabled()) {
					logger.trace("Returning cached Channel: " + this.target);
				}
				this.channelList.addLast(proxy);
			}
		}

		private void physicalClose() throws Exception {
			if (logger.isDebugEnabled()) {
				logger.debug("Closing cached Channel: " + this.target);
			}
			this.target.close();			
		}

	}

}
