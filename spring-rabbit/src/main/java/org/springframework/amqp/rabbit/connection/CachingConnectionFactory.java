/*
 * Copyright 2002-2012 the original author or authors.
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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.LinkedList;
import java.util.List;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannel;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannelImpl;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Channel;

/**
 * A {@link ConnectionFactory} implementation that returns the same Connections from all {@link #createConnection()}
 * calls, and ignores calls to {@link com.rabbitmq.client.Connection#close()} and caches
 * {@link com.rabbitmq.client.Channel}.
 *
 * <p>
 * By default, only one Channel will be cached, with further requested Channels being created and disposed on demand.
 * Consider raising the {@link #setChannelCacheSize(int) "channelCacheSize" value} in case of a high-concurrency
 * environment.
 *
 * <p>
 * <b>NOTE: This ConnectionFactory requires explicit closing of all Channels obtained form its shared Connection.</b>
 * This is the usual recommendation for native Rabbit access code anyway. However, with this ConnectionFactory, its use
 * is mandatory in order to actually allow for Channel reuse.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 */
public class CachingConnectionFactory extends AbstractConnectionFactory {

	private int channelCacheSize = 1;

	private final LinkedList<ChannelProxy> cachedChannelsNonTransactional = new LinkedList<ChannelProxy>();

	private final LinkedList<ChannelProxy> cachedChannelsTransactional = new LinkedList<ChannelProxy>();

	private volatile boolean active = true;

	private ChannelCachingConnectionProxy connection;

	private volatile boolean publisherConfirms;

	private volatile boolean publisherReturns;

	/** Synchronization monitor for the shared Connection */
	private final Object connectionMonitor = new Object();

	/**
	 * Create a new CachingConnectionFactory initializing the hostname to be the value returned from
	 * InetAddress.getLocalHost(), or "localhost" if getLocalHost() throws an exception.
	 */
	public CachingConnectionFactory() {
		this((String) null);
	}

	/**
	 * Create a new CachingConnectionFactory given a host name
	 * and port.
	 *
	 * @param hostname the host name to connect to
	 * @param port the port number
	 */
	public CachingConnectionFactory(String hostname, int port) {
		super(new com.rabbitmq.client.ConnectionFactory());
		if (!StringUtils.hasText(hostname)) {
			hostname = getDefaultHostName();
		}
		setHost(hostname);
		setPort(port);
	}

	/**
	 * Create a new CachingConnectionFactory given a port on the hostname returned from
	 * InetAddress.getLocalHost(), or "localhost" if getLocalHost() throws an exception.
	 *
	 * @param port the port number
	 */
	public CachingConnectionFactory(int port) {
		this(null, port);
	}

	/**
	 * Create a new CachingConnectionFactory given a host name.
	 *
	 * @param hostname the host name to connect to
	 */
	public CachingConnectionFactory(String hostname) {
		this(hostname, com.rabbitmq.client.ConnectionFactory.DEFAULT_AMQP_PORT);
	}

	/**
	 * Create a new CachingConnectionFactory for the given target ConnectionFactory.
	 *
	 * @param rabbitConnectionFactory the target ConnectionFactory
	 */
	public CachingConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory) {
		super(rabbitConnectionFactory);
	}

	public void setChannelCacheSize(int sessionCacheSize) {
		Assert.isTrue(sessionCacheSize >= 1, "Channel cache size must be 1 or higher");
		this.channelCacheSize = sessionCacheSize;
	}

	public int getChannelCacheSize() {
		return this.channelCacheSize;
	}

	public boolean isPublisherConfirms() {
		return publisherConfirms;
	}

	public boolean isPublisherReturns() {
		return publisherReturns;
	}

	public void setPublisherReturns(boolean publisherReturns) {
		this.publisherReturns = publisherReturns;
	}

	public void setPublisherConfirms(boolean publisherConfirms) {
		this.publisherConfirms = publisherConfirms;
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

	private Channel getChannel(boolean transactional) {
		LinkedList<ChannelProxy> channelList = transactional ? this.cachedChannelsTransactional
				: this.cachedChannelsNonTransactional;
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
		} else {
			channel = getCachedChannelProxy(channelList, transactional);
		}
		return channel;
	}

	private ChannelProxy getCachedChannelProxy(LinkedList<ChannelProxy> channelList, boolean transactional) {
		Channel targetChannel = createBareChannel(transactional);
		if (logger.isDebugEnabled()) {
			logger.debug("Creating cached Rabbit Channel from " + targetChannel);
		}
		getChannelListener().onCreate(targetChannel, transactional);
		Class<?>[] interfaces;
		if (this.publisherConfirms || this.publisherReturns) {
			interfaces = new Class[] { ChannelProxy.class, PublisherCallbackChannel.class };
		}
		else {
			interfaces = new Class[] { ChannelProxy.class };
		}
		return (ChannelProxy) Proxy.newProxyInstance(ChannelProxy.class.getClassLoader(),
				interfaces, new CachedChannelInvocationHandler(targetChannel, channelList,
						transactional));
	}

	private Channel createBareChannel(boolean transactional) {
		if (this.connection == null || !this.connection.isOpen()) {
			this.connection = null;
			// Use createConnection here not doCreateConnection so that the old one is properly disposed
			createConnection();
		}
		Channel channel = this.connection.createBareChannel(transactional);
		if (this.publisherConfirms) {
			try {
				channel.confirmSelect();
			} catch (IOException e) {
				logger.error("Could not configure the channel to receive publisher confirms", e);
			}
		}
		if (this.publisherConfirms || this.publisherReturns) {
			if (!(channel instanceof PublisherCallbackChannelImpl)) {
				channel = new PublisherCallbackChannelImpl(channel);
			}
		}
		return channel;
	}

	public final Connection createConnection() throws AmqpException {
		synchronized (this.connectionMonitor) {
			if (this.connection == null) {
				this.connection = new ChannelCachingConnectionProxy(super.createBareConnection());
				// invoke the listener *after* this.connection is assigned
				getConnectionListener().onCreate(connection);
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
		reset();
	}

	/**
	 * Reset the Channel cache and underlying shared Connection, to be reinitialized on next access.
	 */
	protected void reset() {
		this.active = false;
		synchronized (this.cachedChannelsNonTransactional) {
			for (ChannelProxy channel : cachedChannelsNonTransactional) {
				try {
					channel.getTargetChannel().close();
				} catch (Throwable ex) {
					logger.trace("Could not close cached Rabbit Channel", ex);
				}
			}
			this.cachedChannelsNonTransactional.clear();
		}
		synchronized (this.cachedChannelsTransactional) {
			for (ChannelProxy channel : cachedChannelsTransactional) {
				try {
					channel.getTargetChannel().close();
				} catch (Throwable ex) {
					logger.trace("Could not close cached Rabbit Channel", ex);
				}
			}
			this.cachedChannelsTransactional.clear();
		}
		this.active = true;
		this.connection = null;
	}

	@Override
	public String toString() {
		return "CachingConnectionFactory [channelCacheSize=" + channelCacheSize + ", host=" + this.getHost()
				+ ", port=" + this.getPort() + ", active=" + active + "]";
	}

	private class CachedChannelInvocationHandler implements InvocationHandler {

		private volatile Channel target;

		private final LinkedList<ChannelProxy> channelList;

		private final Object targetMonitor = new Object();

		private final boolean transactional;

		public CachedChannelInvocationHandler(Channel target, LinkedList<ChannelProxy> channelList,
				boolean transactional) {
			this.target = target;
			this.channelList = channelList;
			this.transactional = transactional;
		}

		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String methodName = method.getName();
			if (methodName.equals("txSelect") && !this.transactional) {
				throw new UnsupportedOperationException("Cannot start transaction on non-transactional channel");
			}
			if (methodName.equals("equals")) {
				// Only consider equal when proxies are identical.
				return (proxy == args[0]);
			} else if (methodName.equals("hashCode")) {
				// Use hashCode of Channel proxy.
				return System.identityHashCode(proxy);
			} else if (methodName.equals("toString")) {
				return "Cached Rabbit Channel: " + this.target;
			} else if (methodName.equals("close")) {
				// Handle close method: don't pass the call on.
				if (active) {
					synchronized (this.channelList) {
						if (!RabbitUtils.isMustPhysicallyClose() && this.channelList.size() < getChannelCacheSize()) {
							logicalClose((ChannelProxy) proxy);
							// Remain open in the channel list.
							return null;
						}
					}
				}

				// If we get here, we're supposed to shut down.
				physicalClose();
				return null;
			} else if (methodName.equals("getTargetChannel")) {
				// Handle getTargetChannel method: return underlying Channel.
				return this.target;
			} else if (methodName.equals("isOpen")) {
				// Handle isOpen method: we are closed if the target is closed
				return this.target != null && this.target.isOpen();
			}
			try {
				if (this.target == null || !this.target.isOpen()) {
					this.target = null;
				}
				synchronized (targetMonitor) {
					if (this.target == null) {
						this.target = createBareChannel(transactional);
					}
					return method.invoke(this.target, args);
				}
			} catch (InvocationTargetException ex) {
				if (this.target == null || !this.target.isOpen()) {
					// Basic re-connection logic...
					this.target = null;
					logger.debug("Detected closed channel on exception.  Re-initializing: " + target);
					synchronized (targetMonitor) {
						if (this.target == null) {
							this.target = createBareChannel(transactional);
						}
					}
				}
				throw ex.getTargetException();
			}
		}

		/**
		 * GUARDED by channelList
		 *
		 * @param proxy the channel to close
		 */
		private void logicalClose(ChannelProxy proxy) throws Exception {
			if (this.target != null && !this.target.isOpen()) {
				synchronized (targetMonitor) {
					if (this.target != null && !this.target.isOpen()) {
						this.target = null;
						return;
					}
				}
			}
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
			if (this.target == null) {
				return;
			}
			if (this.target.isOpen()) {
				synchronized (targetMonitor) {
					if (this.target.isOpen()) {
						this.target.close();
					}
					this.target = null;
				}
			}
		}

	}

	private class ChannelCachingConnectionProxy implements Connection, ConnectionProxy {

		private volatile Connection target;

		public ChannelCachingConnectionProxy(Connection target) {
			this.target = target;
		}

		private Channel createBareChannel(boolean transactional) {
			return target.createChannel(transactional);
		}

		public Channel createChannel(boolean transactional) {
			Channel channel = getChannel(transactional);
			return channel;
		}

		public void close() {
		}

		public void destroy() {
			reset();
			if (this.target != null) {
				getConnectionListener().onClose(target);
				RabbitUtils.closeConnection(this.target);
			}
			this.target = null;
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
			ChannelCachingConnectionProxy other = (ChannelCachingConnectionProxy) obj;
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
