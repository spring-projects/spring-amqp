/*
 * Copyright 2002-2015 the original author or authors.
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
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannel;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannelImpl;
import org.springframework.amqp.support.ConditionalExceptionLogger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * A {@link ConnectionFactory} implementation that (when the cache mode is {@link CacheMode#CHANNEL} (default)
 * returns the same Connection from all {@link #createConnection()}
 * calls, and ignores calls to {@link com.rabbitmq.client.Connection#close()} and caches
 * {@link com.rabbitmq.client.Channel}.
 * <p>
 * By default, only one Channel will be cached, with further requested Channels being created and disposed on demand.
 * Consider raising the {@link #setChannelCacheSize(int) "channelCacheSize" value} in case of a high-concurrency
 * environment.
 * <p>
 * When the cache mode is {@link CacheMode#CONNECTION}, a new (or cached) connection is used for each {@link #createConnection()};
 * connections are cached according to the {@link #setConnectionCacheSize(int) "connectionCacheSize" value}.
 * Both connections and channels are cached in this mode.
 * <p>
 * <b>{@link CacheMode#CONNECTION} is not compatible with a Rabbit Admin that auto-declares queues etc.</b>
 * <p>
 * <b>NOTE: This ConnectionFactory requires explicit closing of all Channels obtained form its Connection(s).</b>
 * This is the usual recommendation for native Rabbit access code anyway. However, with this ConnectionFactory, its use
 * is mandatory in order to actually allow for Channel reuse. {@link Channel#close()} returns the channel to the
 * cache, if there is room, or physically closes the channel otherwise.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 * @author Steve Powell
 */
public class CachingConnectionFactory extends AbstractConnectionFactory
		implements InitializingBean, ShutdownListener, ApplicationContextAware, ApplicationListener<ContextClosedEvent> {

	private ApplicationContext applicationContext;

	public enum CacheMode {
		/**
		 * Cache channels - single connection
		 */
		CHANNEL,
		/**
		 * Cache connections and channels within each connection
		 */
		CONNECTION
	}

	private final Set<ChannelCachingConnectionProxy> openConnections = new HashSet<ChannelCachingConnectionProxy>();

	private final Map<ChannelCachingConnectionProxy, LinkedList<ChannelProxy>>
			openConnectionNonTransactionalChannels = new HashMap<ChannelCachingConnectionProxy, LinkedList<ChannelProxy>>();

	private final Map<ChannelCachingConnectionProxy, LinkedList<ChannelProxy>>
			openConnectionTransactionalChannels = new HashMap<ChannelCachingConnectionProxy, LinkedList<ChannelProxy>>();

	private final BlockingQueue<ChannelCachingConnectionProxy> idleConnections = new LinkedBlockingQueue<ChannelCachingConnectionProxy>();

	private final Map<Connection, Semaphore> checkoutPermits = new HashMap<Connection, Semaphore>();

	private volatile long channelCheckoutTimeout = 0;

	private volatile CacheMode cacheMode = CacheMode.CHANNEL;

	private volatile int channelCacheSize = 1;

	private volatile int connectionCacheSize = 1;

	private final LinkedList<ChannelProxy> cachedChannelsNonTransactional = new LinkedList<ChannelProxy>();

	private final LinkedList<ChannelProxy> cachedChannelsTransactional = new LinkedList<ChannelProxy>();

	private volatile boolean active = true;

	private volatile ChannelCachingConnectionProxy connection;

	private volatile boolean publisherConfirms;

	private volatile boolean publisherReturns;

	private volatile boolean initialized;

	private volatile boolean stopped;

	private volatile ConditionalExceptionLogger closeExceptionLogger = new DefaultChannelCloseLogger();

	/** Synchronization monitor for the shared Connection */
	private final Object connectionMonitor = new Object();

	/** Executor used for deferred close if no explicit executor set. */
	private final ExecutorService deferredCloseExecutor = Executors.newCachedThreadPool();


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
	 * Create a new CachingConnectionFactory given a {@link URI}.
	 * @param uri the amqp uri configuring the connection
	 * @since 1.5
	 */
	public CachingConnectionFactory(URI uri) {
		super(new com.rabbitmq.client.ConnectionFactory());
		setUri(uri);
	}

	/**
	 * Create a new CachingConnectionFactory given a port on the hostname returned from
	 * InetAddress.getLocalHost(), or "localhost" if getLocalHost() throws an exception.
	 * @param port the port number
	 */
	public CachingConnectionFactory(int port) {
		this(null, port);
	}

	/**
	 * Create a new CachingConnectionFactory given a host name.
	 * @param hostname the host name to connect to
	 */
	public CachingConnectionFactory(String hostname) {
		this(hostname, com.rabbitmq.client.ConnectionFactory.DEFAULT_AMQP_PORT);
	}

	/**
	 * Create a new CachingConnectionFactory for the given target ConnectionFactory.
	 * @param rabbitConnectionFactory the target ConnectionFactory
	 */
	public CachingConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory) {
		super(rabbitConnectionFactory);
	}

	/**
	 * The number of channels to maintain in the cache. By default, channels are allocated on
	 * demand (unbounded) and this represents the maximum cache size. To limit the available
	 * channels, see {@link #setChannelCheckoutTimeout(long)}.
	 * @param sessionCacheSize the channel cache size.
	 * @see #setChannelCheckoutTimeout(long)
	 */
	public void setChannelCacheSize(int sessionCacheSize) {
		Assert.isTrue(sessionCacheSize >= 1, "Channel cache size must be 1 or higher");
		this.channelCacheSize = sessionCacheSize;
	}

	public int getChannelCacheSize() {
		return this.channelCacheSize;
	}

	public CacheMode getCacheMode() {
		return cacheMode;
	}

	public void setCacheMode(CacheMode cacheMode) {
		Assert.isTrue(!initialized, "'cacheMode' cannot be changed after initialization.");
		Assert.notNull(cacheMode, "'cacheMode' must not be null.");
		this.cacheMode = cacheMode;
	}

	@Deprecated
	public int getConnectionCachesize() {
		return getConnectionCacheSize();
	}

	public int getConnectionCacheSize() {
		return this.connectionCacheSize;
	}

	public void setConnectionCacheSize(int connectionCacheSize) {
		Assert.isTrue(connectionCacheSize >= 1, "Connection cache size must be 1 or higher.");
		this.connectionCacheSize = connectionCacheSize;
	}

	public boolean isPublisherConfirms() {
		return this.publisherConfirms;
	}

	public boolean isPublisherReturns() {
		return this.publisherReturns;
	}

	public void setPublisherReturns(boolean publisherReturns) {
		this.publisherReturns = publisherReturns;
	}

	public void setPublisherConfirms(boolean publisherConfirms) {
		this.publisherConfirms = publisherConfirms;
	}

	/**
	 * Sets the channel checkout timeout. When greater than 0, enables channel limiting
	 * in that the {@link #channelCacheSize} becomes the total number of available channels per
	 * connection rather than a simple cache size. Note that changing the {@link #channelCacheSize}
	 * does not affect the limit on existing connection(s), invoke {@link #destroy()} to cause a
	 * new connection to be created with the new limit.
	 * @param channelCheckoutTimeout the timeout in milliseconds; default 0 (channel limiting not enabled).
	 * @since 1.4.2
	 */
	public void setChannelCheckoutTimeout(long channelCheckoutTimeout) {
		this.channelCheckoutTimeout = channelCheckoutTimeout;
	}

	/**
	 * Set the strategy for logging close exceptions; by default, if a channel is closed due to a failed
	 * passive queue declaration, it is logged at debug level. Normal channel closes (200 OK) are not
	 * logged. All others are logged at ERROR level (unless access is refused due to an exclusive consumer
	 * condition, in which case, it is logged at INFO level).
	 * @param closeExceptionLogger the {@link ConditionalExceptionLogger}.
	 * @since 1.5
	 */
	public void setCloseExceptionLogger(ConditionalExceptionLogger closeExceptionLogger) {
		Assert.notNull(closeExceptionLogger, "'closeExceptionLogger' cannot be null");
		this.closeExceptionLogger = closeExceptionLogger;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.initialized = true;
		if (this.cacheMode == CacheMode.CHANNEL) {
			Assert.isTrue(this.connectionCacheSize == 1,
					"When the cache mode is 'CHANNEL', the connection cache size cannot be configured.");
		}
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

	@Override
	public void shutdownCompleted(ShutdownSignalException cause) {
		this.closeExceptionLogger.log(logger, "Channel shutdown" ,cause);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void onApplicationEvent(ContextClosedEvent event) {
		if (this.applicationContext == event.getApplicationContext()) {
			this.stopped = true;
			this.deferredCloseExecutor.shutdownNow();
		}
	}

	private Channel getChannel(ChannelCachingConnectionProxy connection, boolean transactional) {
		if (this.channelCheckoutTimeout > 0) {
			Semaphore checkoutPermits = this.checkoutPermits.get(connection);
			if (checkoutPermits != null) {
				try {
					if (!checkoutPermits.tryAcquire(this.channelCheckoutTimeout, TimeUnit.MILLISECONDS)) {
						throw new AmqpTimeoutException("No available channels");
					}
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new AmqpTimeoutException("Interrupted while acquiring a channel", e);
				}
			}
		}
		LinkedList<ChannelProxy> channelList;
		if (this.cacheMode == CacheMode.CHANNEL) {
			channelList = transactional ? this.cachedChannelsTransactional
					: this.cachedChannelsNonTransactional;
		}
		else {
			channelList = transactional ? this.openConnectionTransactionalChannels.get(connection)
					: this.openConnectionNonTransactionalChannels.get(connection);
		}
		if (channelList == null) {
			channelList = new LinkedList<ChannelProxy>();
			if (transactional) {
				this.openConnectionTransactionalChannels.put(connection, channelList);
			}
			else {
				this.openConnectionNonTransactionalChannels.put(connection, channelList);
			}
		}
		ChannelProxy channel = null;
		if (connection.isOpen()) {
			synchronized (channelList) {
				while (!channelList.isEmpty()) {
					channel = channelList.removeFirst();
					if (logger.isTraceEnabled()) {
						logger.trace(channel + " retrieved from cache");
					}
					if (channel.isOpen()) {
						break;
					}
					else {
						try {
							Channel target = channel.getTargetChannel();
							if (target != null) {
								target.close(); // to remove it from auto-recovery if so configured
							}
						}
						catch (AlreadyClosedException e) {
							if (logger.isTraceEnabled()) {
								logger.trace(channel + " is already closed");
							}
						}
						catch (IOException e) {
							if (logger.isDebugEnabled()) {
								logger.debug("Unexpected Exception closing channel " + e.getMessage());
							}
						}
						catch (TimeoutException e) {
							if (logger.isWarnEnabled()) {
								logger.warn("TimeoutException closing channel " + e.getMessage());
							}
						}
						channel = null;
					}
				}
			}
			if (channel != null) {
				if (logger.isTraceEnabled()) {
					logger.trace("Found cached Rabbit Channel: " + channel.toString());
				}
			}
		}
		if (channel == null) {
			channel = getCachedChannelProxy(connection, channelList, transactional);
		}
		return channel;
	}

	private ChannelProxy getCachedChannelProxy(ChannelCachingConnectionProxy connection,
			LinkedList<ChannelProxy> channelList, boolean transactional) {
		Channel targetChannel = createBareChannel(connection, transactional);
		if (logger.isDebugEnabled()) {
			logger.debug("Creating cached Rabbit Channel from " + targetChannel);
		}
		getChannelListener().onCreate(targetChannel, transactional);
		Class<?>[] interfaces;
		if (this.publisherConfirms || this.publisherReturns) {
			interfaces = new Class<?>[] { ChannelProxy.class, PublisherCallbackChannel.class };
		}
		else {
			interfaces = new Class<?>[] { ChannelProxy.class };
		}
		return (ChannelProxy) Proxy.newProxyInstance(ChannelProxy.class.getClassLoader(),
				interfaces, new CachedChannelInvocationHandler(connection, targetChannel, channelList,
						transactional));
	}

	private Channel createBareChannel(ChannelCachingConnectionProxy connection, boolean transactional) {
		if (this.cacheMode == CacheMode.CHANNEL) {
			if (this.connection == null || !this.connection.isOpen()) {
				synchronized (this.connectionMonitor) {
					if (this.connection != null && !this.connection.isOpen()) {
						this.connection.notifyCloseIfNecessary();
						this.checkoutPermits.remove(this.connection);
					}
					if (this.connection == null || !this.connection.isOpen()) {
						this.connection = null;
						createConnection();
					}
				}
			}
			return doCreateBareChannel(this.connection, transactional);
		}
		else if (this.cacheMode == CacheMode.CONNECTION) {
			if (!connection.isOpen()) {
				synchronized(connectionMonitor) {
					this.openConnectionNonTransactionalChannels.get(connection).clear();
					this.openConnectionTransactionalChannels.get(connection).clear();
					connection.notifyCloseIfNecessary();
					ChannelCachingConnectionProxy newConnection = (ChannelCachingConnectionProxy) createConnection();
					/*
					 * Applications already have a reference to the proxy, so we steal the new (or idle) connection's
					 * target and remove the connection from the open list.
					 */
					connection.target = newConnection.target;
					connection.closeNotified.set(false);
					this.openConnections.remove(newConnection);
				}
			}
			return doCreateBareChannel(connection, transactional);
		}
		return null;
	}

	private Channel doCreateBareChannel(ChannelCachingConnectionProxy connection, boolean transactional) {
		Channel channel = connection.createBareChannel(transactional);
		if (this.publisherConfirms) {
			try {
				channel.confirmSelect();
			}
			catch (IOException e) {
				logger.error("Could not configure the channel to receive publisher confirms", e);
			}
		}
		if (this.publisherConfirms || this.publisherReturns) {
			if (!(channel instanceof PublisherCallbackChannelImpl)) {
				channel = new PublisherCallbackChannelImpl(channel);
			}
		}
		if (channel != null) {
			channel.addShutdownListener(this);
		}
		return channel;
	}

	@Override
	public final Connection createConnection() throws AmqpException {
		Assert.state(!this.stopped, "The ApplicationContext is closed and the ConnectionFactory can no longer create connections.");
		synchronized (this.connectionMonitor) {
			if (this.cacheMode == CacheMode.CHANNEL) {
				if (this.connection == null) {
					this.connection = new ChannelCachingConnectionProxy(super.createBareConnection());
					// invoke the listener *after* this.connection is assigned
					getConnectionListener().onCreate(connection);
					this.checkoutPermits.put(this.connection, new Semaphore(this.channelCacheSize));
				}
				return this.connection;
			}
			else if (this.cacheMode == CacheMode.CONNECTION) {
				ChannelCachingConnectionProxy connection = null;
				while (connection == null && !this.idleConnections.isEmpty()) {
					connection = this.idleConnections.poll();
					if (connection != null) {
						if (!connection.isOpen()) {
							if (logger.isDebugEnabled()) {
								logger.debug("Removing closed connection '" + connection + "'");
							}
							connection.notifyCloseIfNecessary();
							this.openConnections.remove(connection);
							this.openConnectionNonTransactionalChannels.remove(connection);
							this.openConnectionTransactionalChannels.remove(connection);
							this.checkoutPermits.remove(connection);
							connection = null;
						}
					}
				}
				if (connection == null) {
					connection = new ChannelCachingConnectionProxy(super.createBareConnection());
					getConnectionListener().onCreate(connection);
					if (logger.isDebugEnabled()) {
						logger.debug("Adding new connection '" + connection + "'");
					}
					this.openConnections.add(connection);
					this.openConnectionNonTransactionalChannels.put(connection, new LinkedList<ChannelProxy>());
					this.openConnectionTransactionalChannels.put(connection, new LinkedList<ChannelProxy>());
					this.checkoutPermits.put(connection, new Semaphore(this.channelCacheSize));
				}
				else {
					if (logger.isDebugEnabled()) {
						logger.debug("Obtained connection '" + connection + "' from cache");
					}
				}
				return connection;
			}
		}
		return null;
	}

	/**
	 * Close the underlying shared connection. The provider of this ConnectionFactory needs to care for proper shutdown.
	 * <p>
	 * As this bean implements DisposableBean, a bean factory will automatically invoke this on destruction of its
	 * cached singletons.
	 */
	@Override
	public final void destroy() {
		resetConnection();
	}

	/**
	 * Close the connection(s). This will impact any in-process operations. New
	 * connection(s) will be created on demand after this method returns. This might be
	 * used to force a reconnect to the primary broker after failing over to a secondary
	 * broker.
	 */
	public void resetConnection() {
		synchronized (this.connectionMonitor) {
			if (connection != null) {
				this.connection.destroy();
				this.checkoutPermits.remove(this.connection);
				this.connection = null;
			}
			for (ChannelCachingConnectionProxy connection : this.openConnections) {
				connection.destroy();
				this.checkoutPermits.remove(connection);
			}
			this.openConnections.clear();
			this.idleConnections.clear();
			this.openConnectionNonTransactionalChannels.clear();
			this.openConnectionTransactionalChannels.clear();
		}
	}

	/*
	 * Reset the Channel cache and underlying shared Connection, to be reinitialized on next access.
	 */
	protected void reset(List<ChannelProxy> channels, List<ChannelProxy> txChannels) {
		this.active = false;
		if (this.cacheMode == CacheMode.CHANNEL) {
			synchronized (channels) {
				for (ChannelProxy channel : channels) {
					try {
						channel.close();
					}
					catch (Exception ex) {
						logger.trace("Could not close cached Rabbit Channel", ex);
					}
				}
				channels.clear();
			}
			synchronized (txChannels) {
				for (ChannelProxy channel : txChannels) {
					try {
						channel.close();
					}
					catch (Exception ex) {
						logger.trace("Could not close cached Rabbit Channel", ex);
					}
				}
				txChannels.clear();
			}
		}
		this.active = true;
		this.connection = null;
	}

	@Override
	public String toString() {
		return "CachingConnectionFactory [channelCacheSize=" + channelCacheSize + ", host=" + this.getHost()
				+ ", port=" + this.getPort() + ", active=" + active
				+ " " + super.toString() + "]";
	}

	private class CachedChannelInvocationHandler implements InvocationHandler {

		private final ChannelCachingConnectionProxy theConnection;

		private volatile Channel target;

		private final LinkedList<ChannelProxy> channelList;

		private final Object targetMonitor = new Object();

		private final boolean transactional;

		public CachedChannelInvocationHandler(ChannelCachingConnectionProxy connection,
				Channel target,
				LinkedList<ChannelProxy> channelList,
				boolean transactional) {
			this.theConnection = connection;
			this.target = target;
			this.channelList = channelList;
			this.transactional = transactional;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String methodName = method.getName();
			if (methodName.equals("txSelect") && !this.transactional) {
				throw new UnsupportedOperationException("Cannot start transaction on non-transactional channel");
			}
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
						if (!RabbitUtils.isPhysicalCloseRequired() &&
								(this.channelList.size() < getChannelCacheSize()
										|| this.channelList.contains(proxy))) {
							logicalClose((ChannelProxy) proxy);
							// Remain open in the channel list.
							releasePermit();
							return null;
						}
					}
				}

				// If we get here, we're supposed to shut down.
				physicalClose();
				releasePermit();
				return null;
			}
			else if (methodName.equals("getTargetChannel")) {
				// Handle getTargetChannel method: return underlying Channel.
				return this.target;
			}
			else if (methodName.equals("isOpen")) {
				// Handle isOpen method: we are closed if the target is closed
				return this.target != null && this.target.isOpen();
			}
			else if (methodName.equals("isTransactional")) {
				return this.transactional;
			}
			try {
				if (this.target == null || !this.target.isOpen()) {
					this.target = null;
				}
				synchronized (targetMonitor) {
					if (this.target == null) {
						this.target = createBareChannel(theConnection, transactional);
					}
					return method.invoke(this.target, args);
				}
			}
			catch (InvocationTargetException ex) {
				if (this.target == null || !this.target.isOpen()) {
					// Basic re-connection logic...
					this.target = null;
					if (logger.isDebugEnabled()) {
						logger.debug("Detected closed channel on exception.  Re-initializing: " + target);
					}
					synchronized (targetMonitor) {
						if (this.target == null) {
							this.target = createBareChannel(theConnection, transactional);
						}
					}
				}
				throw ex.getTargetException();
			}
		}

		private void releasePermit() {
			if (CachingConnectionFactory.this.channelCheckoutTimeout > 0) {
				Semaphore checkoutPermits = CachingConnectionFactory.this.checkoutPermits.get(this.theConnection);
				if (checkoutPermits != null) {
					checkoutPermits.release();
				}
			}
		}

		/**
		 * GUARDED by channelList
		 * @param proxy the channel to close
		 */
		private void logicalClose(ChannelProxy proxy) throws Exception {
			if (target == null) {
				return;
			}
			if (this.target != null && !this.target.isOpen()) {
				synchronized (targetMonitor) {
					if (this.target != null && !this.target.isOpen()) {
						if (this.channelList.contains(proxy)) {
							this.channelList.remove(proxy);
						}
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
			try {
				if (CachingConnectionFactory.this.active &&
						(CachingConnectionFactory.this.publisherConfirms ||
								CachingConnectionFactory.this.publisherReturns)) {
					ExecutorService executorService = (getExecutorService() != null
							? getExecutorService()
							: CachingConnectionFactory.this.deferredCloseExecutor);
					final Channel channel = CachedChannelInvocationHandler.this.target;
					executorService.execute(new Runnable() {

						@Override
						public void run() {
							try {
								if (CachingConnectionFactory.this.publisherConfirms) {
									channel.waitForConfirmsOrDie(5000);
								}
								else {
									Thread.sleep(5000);
								}
							}
							catch (InterruptedException e) {
								Thread.currentThread().interrupt();
							}
							catch (Exception e) {}
							finally {
								try {
									if (channel.isOpen()) {
										channel.close();
									}
								}
								catch (IOException e) {}
								catch (AlreadyClosedException e) {}
								catch (TimeoutException e) {}
							}
						}

					});
				}
				else {
					this.target.close();
				}
			}
			catch (AlreadyClosedException e) {
				if (logger.isTraceEnabled()) {
					logger.trace(this.target + " is already closed");
				}
			}
			finally {
				this.target = null;
			}
		}

	}

	private class ChannelCachingConnectionProxy implements Connection, ConnectionProxy {

		private volatile Connection target;

		private final AtomicBoolean closeNotified = new AtomicBoolean(false);

		public ChannelCachingConnectionProxy(Connection target) {
			this.target = target;
		}

		private Channel createBareChannel(boolean transactional) {
			return target.createChannel(transactional);
		}

		@Override
		public Channel createChannel(boolean transactional) {
			return getChannel(this, transactional);
		}

		@Override
		public void close() {
			if (cacheMode == CacheMode.CONNECTION) {
				synchronized (connectionMonitor) {
					if (!this.target.isOpen() || idleConnections.size() >= connectionCacheSize) {
						if (logger.isDebugEnabled()) {
							logger.debug("Completely closing connection '" + this + "'");
						}
						if (this.target.isOpen()) {
							RabbitUtils.closeConnection(this.target);
						}
						this.notifyCloseIfNecessary();
						openConnections.remove(this);
						openConnectionNonTransactionalChannels.remove(this);
						openConnectionTransactionalChannels.remove(this);
					}
					else {
						if (!idleConnections.contains(this)) {
							if (logger.isDebugEnabled()) {
								logger.debug("Returning connection '" + this + "' to cache");
							}
							idleConnections.add(this);
						}
					}
				}
			}
		}

		public void destroy() {
			if (CachingConnectionFactory.this.cacheMode == CacheMode.CHANNEL) {
				reset(cachedChannelsNonTransactional, cachedChannelsTransactional);
			}
			else {
				reset(openConnectionNonTransactionalChannels.get(this), openConnectionTransactionalChannels.get(this));
			}
			if (this.target != null) {
				RabbitUtils.closeConnection(this.target);
				this.notifyCloseIfNecessary();
			}
			this.target = null;
		}

		private void notifyCloseIfNecessary() {
			if (!(this.closeNotified.getAndSet(true))) {
				getConnectionListener().onClose(this);
			}
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
			ChannelCachingConnectionProxy other = (ChannelCachingConnectionProxy) obj;
			if (target == null) {
				if (other.target != null) {
					return false;
				}
			} else if (!target.equals(other.target)) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return cacheMode == CacheMode.CHANNEL ? "Shared " : "Dedicated " +
					"Rabbit Connection: " + this.target;
		}

	}

	/**
	 * Default implementation of {@link ConditionalExceptionLogger} for logging channel
	 * close exceptions.
	 * @since 1.5
	 */
	private static class DefaultChannelCloseLogger implements ConditionalExceptionLogger {

		@Override
		public void log(Log logger, String message, Throwable t) {
			if (t instanceof ShutdownSignalException) {
				ShutdownSignalException cause = (ShutdownSignalException) t;
				if (RabbitUtils.isPassiveDeclarationChannelClose(cause)) {
					if (logger.isDebugEnabled()) {
						logger.debug(message + ": " + cause.getMessage());
					}
				}
				else if (RabbitUtils.isExclusiveUseChannelClose(cause)) {
					if (logger.isInfoEnabled()) {
						logger.info(message + ": " + cause.getMessage());
					}
				}
				else if (!RabbitUtils.isNormalChannelClose(cause)) {
					logger.error(message + ": " + cause.getMessage());
				}
			}
			else {
				logger.error("Unexpected invocation of " + this.getClass() + ", with message: " + message, t);
			}
		}

	}

}
