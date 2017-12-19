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

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;

import org.springframework.amqp.AmqpApplicationContextClosedException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannel;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannelImpl;
import org.springframework.amqp.support.ConditionalExceptionLogger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BlockedListener;
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
@ManagedResource
public class CachingConnectionFactory extends AbstractConnectionFactory
		implements InitializingBean, ShutdownListener, PublisherCallbackChannelConnectionFactory {

	private static final int DEFAULT_CHANNEL_CACHE_SIZE = 25;

	private static final Set<String> txStarts = new HashSet<>(Arrays.asList("basicPublish", "basicAck",
			"basicNack", "basicReject"));

	private static final Set<String> ackMethods = new HashSet<>(Arrays.asList("basicAck",
			"basicNack", "basicReject"));

	private static final Set<String> txEnds = new HashSet<>(Arrays.asList("txCommit", "txRollback"));

	private final ChannelCachingConnectionProxy connection = new ChannelCachingConnectionProxy(null);

	/**
	 * The cache mode.
	 */
	public enum CacheMode {
		/**
		 * Cache channels - single connection.
		 */
		CHANNEL,
		/**
		 * Cache connections and channels within each connection.
		 */
		CONNECTION
	}

	private final Set<ChannelCachingConnectionProxy> allocatedConnections = new HashSet<>();

	private final Map<ChannelCachingConnectionProxy, LinkedList<ChannelProxy>>
		allocatedConnectionNonTransactionalChannels = new HashMap<>();

	private final Map<ChannelCachingConnectionProxy, LinkedList<ChannelProxy>>
		allocatedConnectionTransactionalChannels = new HashMap<>();

	private final BlockingDeque<ChannelCachingConnectionProxy> idleConnections = new LinkedBlockingDeque<>();

	private final LinkedList<ChannelProxy> cachedChannelsNonTransactional = new LinkedList<>();

	private final LinkedList<ChannelProxy> cachedChannelsTransactional = new LinkedList<>();

	private final Map<Connection, Semaphore> checkoutPermits = new HashMap<>();

	private final Map<String, AtomicInteger> channelHighWaterMarks = new HashMap<>();

	private final AtomicInteger connectionHighWaterMark = new AtomicInteger();

	private final CachingConnectionFactory publisherConnectionFactory;

	private volatile long channelCheckoutTimeout = 0;

	private volatile CacheMode cacheMode = CacheMode.CHANNEL;

	private volatile int channelCacheSize = DEFAULT_CHANNEL_CACHE_SIZE;

	private volatile int connectionCacheSize = 1;

	private volatile int connectionLimit = Integer.MAX_VALUE;

	private volatile boolean active = true;

	private volatile boolean publisherConfirms;

	private volatile boolean publisherReturns;

	private volatile boolean initialized;

	private volatile boolean stopped;

	private volatile ConditionalExceptionLogger closeExceptionLogger = new DefaultChannelCloseLogger();

	/** Synchronization monitor for the shared Connection. */
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
	 * Create a new CachingConnectionFactory given a host name.
	 * @param hostname the host name to connect to
	 */
	public CachingConnectionFactory(String hostname) {
		this(hostname, com.rabbitmq.client.ConnectionFactory.DEFAULT_AMQP_PORT);
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
	 * Create a new CachingConnectionFactory given a host name
	 * and port.
	 * @param hostname the host name to connect to
	 * @param port the port number
	 */
	public CachingConnectionFactory(String hostname, int port) {
		super(newRabbitConnectionFactory());
		if (!StringUtils.hasText(hostname)) {
			hostname = getDefaultHostName();
		}
		setHost(hostname);
		setPort(port);
		this.publisherConnectionFactory = new CachingConnectionFactory(getRabbitConnectionFactory(), true);
		setPublisherConnectionFactory(this.publisherConnectionFactory);
	}

	/**
	 * Create a new CachingConnectionFactory given a {@link URI}.
	 * @param uri the amqp uri configuring the connection
	 * @since 1.5
	 */
	public CachingConnectionFactory(URI uri) {
		super(newRabbitConnectionFactory());
		setUri(uri);
		this.publisherConnectionFactory = new CachingConnectionFactory(getRabbitConnectionFactory(), true);
		setPublisherConnectionFactory(this.publisherConnectionFactory);
	}

	/**
	 * Create a new CachingConnectionFactory for the given target ConnectionFactory.
	 * @param rabbitConnectionFactory the target ConnectionFactory
	 */
	public CachingConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory) {
		this(rabbitConnectionFactory, false);
	}

	/**
	 * Create a new CachingConnectionFactory for the given target ConnectionFactory.
	 * @param rabbitConnectionFactory the target ConnectionFactory
	 * @param isPublisherFactory true if this is the publisher sub-factory.
	 */
	private CachingConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory,
			boolean isPublisherFactory) {
		super(rabbitConnectionFactory);
		if (!isPublisherFactory) {
			if (rabbitConnectionFactory.isAutomaticRecoveryEnabled()) {
				logger.warn("***\nAutomatic Recovery is Enabled in the provided connection factory;\n"
					+ "while Spring AMQP is compatible with this feature, it\n"
					+ "prefers to use its own recovery mechanisms; when this option is true, you may receive\n"
					+ "'AutoRecoverConnectionNotCurrentlyOpenException's until the connection is recovered.");
			}
			this.publisherConnectionFactory = new CachingConnectionFactory(getRabbitConnectionFactory(),
					true);
			setPublisherConnectionFactory(this.publisherConnectionFactory);
		}
		else {
			this.publisherConnectionFactory = null;
		}
	}

	private static com.rabbitmq.client.ConnectionFactory newRabbitConnectionFactory() {
		com.rabbitmq.client.ConnectionFactory connectionFactory = new com.rabbitmq.client.ConnectionFactory();
		connectionFactory.setAutomaticRecoveryEnabled(false);
		return connectionFactory;
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
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setChannelCacheSize(sessionCacheSize);
		}
	}

	public int getChannelCacheSize() {
		return this.channelCacheSize;
	}

	public CacheMode getCacheMode() {
		return this.cacheMode;
	}

	public void setCacheMode(CacheMode cacheMode) {
		Assert.isTrue(!this.initialized, "'cacheMode' cannot be changed after initialization.");
		Assert.notNull(cacheMode, "'cacheMode' must not be null.");
		this.cacheMode = cacheMode;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setCacheMode(cacheMode);
		}
	}

	public int getConnectionCacheSize() {
		return this.connectionCacheSize;
	}

	public void setConnectionCacheSize(int connectionCacheSize) {
		Assert.isTrue(connectionCacheSize >= 1, "Connection cache size must be 1 or higher.");
		this.connectionCacheSize = connectionCacheSize;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setConnectionCacheSize(connectionCacheSize);
		}
	}

	/**
	 * Set the connection limit when using cache mode CONNECTION. When the limit is
	 * reached and there are no idle connections, the
	 * {@link #setChannelCheckoutTimeout(long) channelCheckoutTimeLimit} is used to wait
	 * for a connection to become idle.
	 * @param connectionLimit the limit.
	 * @since 1.5.5
	 */
	public void setConnectionLimit(int connectionLimit) {
		Assert.isTrue(connectionLimit >= 1, "Connection limit must be 1 or higher.");
		this.connectionLimit = connectionLimit;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setConnectionLimit(connectionLimit);
		}
	}

	@Override
	public boolean isPublisherConfirms() {
		return this.publisherConfirms;
	}

	@Override
	public boolean isPublisherReturns() {
		return this.publisherReturns;
	}

	public void setPublisherReturns(boolean publisherReturns) {
		this.publisherReturns = publisherReturns;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setPublisherReturns(publisherReturns);
		}
	}

	public void setPublisherConfirms(boolean publisherConfirms) {
		this.publisherConfirms = publisherConfirms;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setPublisherConfirms(publisherConfirms);
		}
	}

	/**
	 * Sets the channel checkout timeout. When greater than 0, enables channel limiting
	 * in that the {@link #channelCacheSize} becomes the total number of available channels per
	 * connection rather than a simple cache size. Note that changing the {@link #channelCacheSize}
	 * does not affect the limit on existing connection(s), invoke {@link #destroy()} to cause a
	 * new connection to be created with the new limit.
	 * <p>
	 * Since 1.5.5, also applies to getting a connection when the cache mode is CONNECTION.
	 * @param channelCheckoutTimeout the timeout in milliseconds; default 0 (channel limiting not enabled).
	 * @since 1.4.2
	 * @see #setConnectionLimit(int)
	 */
	public void setChannelCheckoutTimeout(long channelCheckoutTimeout) {
		this.channelCheckoutTimeout = channelCheckoutTimeout;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setChannelCheckoutTimeout(channelCheckoutTimeout);
		}
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
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setCloseExceptionLogger(closeExceptionLogger);
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.initialized = true;
		if (this.cacheMode == CacheMode.CHANNEL) {
			Assert.isTrue(this.connectionCacheSize == 1,
					"When the cache mode is 'CHANNEL', the connection cache size cannot be configured.");
		}
		initCacheWaterMarks();
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.afterPropertiesSet();
		}
	}

	private void initCacheWaterMarks() {
		this.channelHighWaterMarks.put(ObjectUtils.getIdentityHexString(this.cachedChannelsNonTransactional),
				new AtomicInteger());
		this.channelHighWaterMarks.put(ObjectUtils.getIdentityHexString(this.cachedChannelsTransactional),
				new AtomicInteger());
	}

	@Override
	public void setConnectionListeners(List<? extends ConnectionListener> listeners) {
		super.setConnectionListeners(listeners); // handles publishing sub-factory
		// If the connection is already alive we assume that the new listeners want to be notified
		if (this.connection.target != null) {
			this.getConnectionListener().onCreate(this.connection);
		}
	}

	@Override
	public void addConnectionListener(ConnectionListener listener) {
		super.addConnectionListener(listener); // handles publishing sub-factory
		// If the connection is already alive we assume that the new listener wants to be notified
		if (this.connection.target != null) {
			listener.onCreate(this.connection);
		}
	}

	@Override
	public void shutdownCompleted(ShutdownSignalException cause) {
		this.closeExceptionLogger.log(logger, "Channel shutdown", cause);
		int protocolClassId = cause.getReason().protocolClassId();
		if (protocolClassId == 20) {
			getChannelListener().onShutDown(cause);
		}
		else if (protocolClassId == 10) {
			getConnectionListener().onShutDown(cause);
		}

	}

	private Channel getChannel(ChannelCachingConnectionProxy connection, boolean transactional) {
		Semaphore checkoutPermits = null;
		if (this.channelCheckoutTimeout > 0) {
			checkoutPermits = this.checkoutPermits.get(connection);
			if (checkoutPermits != null) {
				try {
					if (!checkoutPermits.tryAcquire(this.channelCheckoutTimeout, TimeUnit.MILLISECONDS)) {
						throw new AmqpTimeoutException("No available channels");
					}
					if (logger.isDebugEnabled()) {
						logger.debug(
							"Acquired permit for " + connection + ", remaining:" + checkoutPermits.availablePermits());
					}
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new AmqpTimeoutException("Interrupted while acquiring a channel", e);
				}
			}
			else {
				throw new IllegalStateException("No permits map entry for " + connection);
			}
		}
		LinkedList<ChannelProxy> channelList;
		if (this.cacheMode == CacheMode.CHANNEL) {
			channelList = transactional ? this.cachedChannelsTransactional
					: this.cachedChannelsNonTransactional;
		}
		else {
			channelList = transactional ? this.allocatedConnectionTransactionalChannels.get(connection)
					: this.allocatedConnectionNonTransactionalChannels.get(connection);
		}
		if (channelList == null) {
			throw new IllegalStateException("No channel list for connection " + connection);
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
								target.close();
								/*
								 *  To remove it from auto-recovery if so configured,
								 *  and nack any pending confirms if PublisherCallbackChannel.
								 */
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
			try {
				channel = getCachedChannelProxy(connection, channelList, transactional);
			}
			catch (RuntimeException e) {
				if (checkoutPermits != null) {
					checkoutPermits.release();
					if (logger.isDebugEnabled()) {
						logger.debug("Could not get channel; released permit for " + connection + ", remaining:"
							+ checkoutPermits.availablePermits());
					}
				}
				throw e;
			}
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
			if (!this.connection.isOpen()) {
				synchronized (this.connectionMonitor) {
					if (!this.connection.isOpen()) {
						this.connection.notifyCloseIfNecessary();
					}
					if (!this.connection.isOpen()) {
						this.connection.target = null;
						createConnection();
					}
				}
			}
			return doCreateBareChannel(this.connection, transactional);
		}
		else if (this.cacheMode == CacheMode.CONNECTION) {
			if (!connection.isOpen()) {
				synchronized (this.connectionMonitor) {
					this.allocatedConnectionNonTransactionalChannels.get(connection).clear();
					this.allocatedConnectionTransactionalChannels.get(connection).clear();
					connection.notifyCloseIfNecessary();
					refreshProxyConnection(connection);
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
		if (this.stopped) {
			throw new AmqpApplicationContextClosedException(
					"The ApplicationContext is closed and the ConnectionFactory can no longer create connections.");
		}
		synchronized (this.connectionMonitor) {
			if (this.cacheMode == CacheMode.CHANNEL) {
				if (this.connection.target == null) {
					this.connection.target = super.createBareConnection();
					// invoke the listener *after* this.connection is assigned
					if (!this.checkoutPermits.containsKey(this.connection)) {
						this.checkoutPermits.put(this.connection, new Semaphore(this.channelCacheSize));
					}
					this.connection.closeNotified.set(false);
					getConnectionListener().onCreate(this.connection);
				}
				return this.connection;
			}
			else if (this.cacheMode == CacheMode.CONNECTION) {
				ChannelCachingConnectionProxy connection = findIdleConnection();
				long now = System.currentTimeMillis();
				while (connection == null && System.currentTimeMillis() - now < this.channelCheckoutTimeout) {
					if (countOpenConnections() >= this.connectionLimit) {
						try {
							this.connectionMonitor.wait(this.channelCheckoutTimeout);
							connection = findIdleConnection();
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							throw new AmqpException("Interrupted while waiting for a connection", e);
						}
					}
				}
				if (connection == null) {
					if (countOpenConnections() >= this.connectionLimit
							&& System.currentTimeMillis() - now >= this.channelCheckoutTimeout) {
						throw new AmqpTimeoutException("Timed out attempting to get a connection");
					}
					connection = new ChannelCachingConnectionProxy(super.createBareConnection());
					if (logger.isDebugEnabled()) {
						logger.debug("Adding new connection '" + connection + "'");
					}
					this.allocatedConnections.add(connection);
					this.allocatedConnectionNonTransactionalChannels.put(connection, new LinkedList<ChannelProxy>());
					this.channelHighWaterMarks.put(ObjectUtils.getIdentityHexString(
							this.allocatedConnectionNonTransactionalChannels.get(connection)), new AtomicInteger());
					this.allocatedConnectionTransactionalChannels.put(connection, new LinkedList<ChannelProxy>());
					this.channelHighWaterMarks.put(
							ObjectUtils.getIdentityHexString(this.allocatedConnectionTransactionalChannels.get(connection)),
							new AtomicInteger());
					this.checkoutPermits.put(connection, new Semaphore(this.channelCacheSize));
					getConnectionListener().onCreate(connection);
				}
				else if (!connection.isOpen()) {
					try {
						refreshProxyConnection(connection);
					}
					catch (Exception e) {
						this.idleConnections.addLast(connection);
					}
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

	/*
	 * Iterate over the idle connections looking for an open one. If there are no idle,
	 * return null, if there are no open idle, return the first closed idle so it can
	 * be reopened.
	 */
	private ChannelCachingConnectionProxy findIdleConnection() {
		ChannelCachingConnectionProxy connection = null;
		ChannelCachingConnectionProxy lastIdle = this.idleConnections.peekLast();
		while (connection == null) {
			connection = this.idleConnections.poll();
			if (connection != null) {
				if (!connection.isOpen()) {
					if (logger.isDebugEnabled()) {
						logger.debug("Skipping closed connection '" + connection + "'");
					}
					connection.notifyCloseIfNecessary();
					this.idleConnections.addLast(connection);
					if (connection.equals(lastIdle)) {
						// all of the idle connections are closed.
						connection = this.idleConnections.poll();
						break;
					}
					connection = null;
				}
			}
			else {
				break;
			}
		}
		return connection;
	}

	private void refreshProxyConnection(ChannelCachingConnectionProxy connection) {
		connection.destroy();
		connection.notifyCloseIfNecessary();
		connection.target = super.createBareConnection();
		connection.closeNotified.set(false);
		getConnectionListener().onCreate(connection);
		if (logger.isDebugEnabled()) {
			logger.debug("Refreshed existing connection '" + connection + "'");
		}
	}

	/**
	 * Close the underlying shared connection. Use {@link #resetConnection()} to close the
	 * connection while the application is still running.
	 * <p>
	 * As this bean implements DisposableBean, a bean factory will automatically invoke
	 * this on destruction of its cached singletons.
	 * <p>
	 * If called after the context is closed, the connection factory can no longer server
	 * up connections.
	 */
	@Override
	public final void destroy() {
		super.destroy();
		resetConnection();
		if (getContextStopped()) {
			this.stopped = true;
			this.deferredCloseExecutor.shutdownNow();
		}
	}

	/**
	 * Close the connection(s). This will impact any in-process operations. New
	 * connection(s) will be created on demand after this method returns. This might be
	 * used to force a reconnect to the primary broker after failing over to a secondary
	 * broker.
	 */
	public void resetConnection() {
		synchronized (this.connectionMonitor) {
			if (this.connection.target != null) {
				this.connection.destroy();
			}
			for (ChannelCachingConnectionProxy connection : this.allocatedConnections) {
				connection.destroy();
			}
			for (AtomicInteger count : this.channelHighWaterMarks.values()) {
				count.set(0);
			}
			this.connectionHighWaterMark.set(0);
		}
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.resetConnection();
		}
	}

	/*
	 * Reset the Channel cache and underlying shared Connection, to be reinitialized on next access.
	 */
	protected void reset(List<ChannelProxy> channels, List<ChannelProxy> txChannels) {
		this.active = false;
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
		this.active = true;
	}

	@ManagedAttribute
	public Properties getCacheProperties() {
		Properties props = new Properties();
		props.setProperty("cacheMode", this.cacheMode.name());
		synchronized (this.connectionMonitor) {
			props.setProperty("channelCacheSize", Integer.toString(this.channelCacheSize));
			if (this.cacheMode.equals(CacheMode.CONNECTION)) {
				props.setProperty("connectionCacheSize", Integer.toString(this.connectionCacheSize));
				props.setProperty("openConnections", Integer.toString(countOpenConnections()));
				props.setProperty("idleConnections", Integer.toString(this.idleConnections.size()));
				props.setProperty("idleConnectionsHighWater",  Integer.toString(this.connectionHighWaterMark.get()));
				for (ChannelCachingConnectionProxy proxy : this.allocatedConnections) {
					putConnectionName(props, proxy, ":" + proxy.getLocalPort());
				}
				for (Entry<ChannelCachingConnectionProxy, LinkedList<ChannelProxy>> entry :
										this.allocatedConnectionTransactionalChannels.entrySet()) {
					int port = entry.getKey().getLocalPort();
					if (port > 0 && entry.getKey().isOpen()) {
						LinkedList<ChannelProxy> channelList = entry.getValue();
						props.put("idleChannelsTx:" + port, Integer.toString(channelList.size()));
						props.put("idleChannelsTxHighWater:" + port, Integer.toString(
								this.channelHighWaterMarks.get(ObjectUtils.getIdentityHexString(channelList)).get()));
					}
				}
				for (Entry<ChannelCachingConnectionProxy, LinkedList<ChannelProxy>> entry :
										this.allocatedConnectionNonTransactionalChannels.entrySet()) {
					int port = entry.getKey().getLocalPort();
					if (port > 0 && entry.getKey().isOpen()) {
						LinkedList<ChannelProxy> channelList = entry.getValue();
						props.put("idleChannelsNotTx:" + port, Integer.toString(channelList.size()));
						props.put("idleChannelsNotTxHighWater:" + port, Integer.toString(
								this.channelHighWaterMarks.get(ObjectUtils.getIdentityHexString(channelList)).get()));
					}
				}
			}
			else {
				props.setProperty("localPort",
						Integer.toString(this.connection.target == null ? 0 : this.connection.getLocalPort()));
				props.setProperty("idleChannelsTx", Integer.toString(this.cachedChannelsTransactional.size()));
				props.setProperty("idleChannelsNotTx", Integer.toString(this.cachedChannelsNonTransactional.size()));
				props.setProperty("idleChannelsTxHighWater", Integer.toString(this.channelHighWaterMarks
						.get(ObjectUtils.getIdentityHexString(this.cachedChannelsTransactional)).get()));
				props.setProperty("idleChannelsNotTxHighWater", Integer.toString(this.channelHighWaterMarks
						.get(ObjectUtils.getIdentityHexString(this.cachedChannelsNonTransactional)).get()));
				putConnectionName(props, this.connection, "");
			}
		}
		return props;
	}

	/**
	 * Return the cache properties from the underlying publisher sub-factory.
	 * @return the properties.
	 * @since 2.0.2
	 */
	@ManagedAttribute
	public Properties getPublisherConnectionFactoryCacheProperties() {
		if (this.publisherConnectionFactory != null) {
			return this.publisherConnectionFactory.getCacheProperties();
		}
		return new Properties();
	}

	private void putConnectionName(Properties props, ConnectionProxy connection, String keySuffix) {
		Connection targetConnection = connection.getTargetConnection(); // NOSONAR (close())
		if (targetConnection instanceof SimpleConnection) {
			String name = ((SimpleConnection) targetConnection).getDelegate().getClientProvidedName();
			if (name != null) {
				props.put("connectionName" + keySuffix, name);
			}
		}
	}

	private int countOpenConnections() {
		int n = 0;
		for (ChannelCachingConnectionProxy proxy : this.allocatedConnections) {
			if (proxy.isOpen()) {
				n++;
			}
		}
		return n;
	}

	@Override
	public String toString() {
		return "CachingConnectionFactory [channelCacheSize=" + this.channelCacheSize + ", host=" + getHost()
				+ ", port=" + getPort() + ", active=" + this.active
				+ " " + super.toString() + "]";
	}

	private final class CachedChannelInvocationHandler implements InvocationHandler {

		private final ChannelCachingConnectionProxy theConnection;

		private final LinkedList<ChannelProxy> channelList;

		private final String channelListIdentity;

		private final Object targetMonitor = new Object();

		private final boolean transactional;

		private volatile Channel target;

		private volatile boolean txStarted;

		CachedChannelInvocationHandler(ChannelCachingConnectionProxy connection,
				Channel target,
				LinkedList<ChannelProxy> channelList,
				boolean transactional) {
			this.theConnection = connection;
			this.target = target;
			this.channelList = channelList;
			this.channelListIdentity = ObjectUtils.getIdentityHexString(channelList);
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
				return "Cached Rabbit Channel: " + this.target + ", conn: " + this.theConnection;
			}
			else if (methodName.equals("close")) {
				// Handle close method: don't pass the call on.
				if (CachingConnectionFactory.this.active) {
					synchronized (this.channelList) {
						if (!RabbitUtils.isPhysicalCloseRequired() &&
								(this.channelList.size() < getChannelCacheSize()
										|| this.channelList.contains(proxy))) {
							releasePermitIfNecessary(proxy);
							logicalClose((ChannelProxy) proxy);
							return null;
						}
					}
				}

				// If we get here, we're supposed to shut down.
				physicalClose();
				releasePermitIfNecessary(proxy);
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
					if (this.target instanceof PublisherCallbackChannel) {
						this.target.close();
						throw new InvocationTargetException(
								new AmqpException("PublisherCallbackChannel is closed"));
					}
					else if (this.txStarted) {
						this.txStarted = false;
						throw new InvocationTargetException(
								new IllegalStateException("Channel closed during transaction"));
					}
					else if (ackMethods.contains(methodName)) {
						throw new InvocationTargetException(
								new IllegalStateException("Channel closed; cannot ack/nack"));
					}
					this.target = null;
				}
				synchronized (this.targetMonitor) {
					if (this.target == null) {
						this.target = createBareChannel(this.theConnection, this.transactional);
					}
					Object result = method.invoke(this.target, args);
					if (this.transactional) {
						if (txStarts.contains(methodName)) {
							this.txStarted = true;
						}
						else if (txEnds.contains(methodName)) {
							this.txStarted = false;
						}
					}
					return result;
				}
			}
			catch (InvocationTargetException ex) {
				if (this.target == null || !this.target.isOpen()) {
					// Basic re-connection logic...
					if (logger.isDebugEnabled()) {
						logger.debug("Detected closed channel on exception.  Re-initializing: " + this.target);
					}
					this.target = null;
					synchronized (this.targetMonitor) {
						if (this.target == null) {
							this.target = createBareChannel(this.theConnection, this.transactional);
						}
					}
				}
				throw ex.getTargetException();
			}
		}

		private void releasePermitIfNecessary(Object proxy) {
			if (CachingConnectionFactory.this.channelCheckoutTimeout > 0) {
				/*
				 *  Only release a permit if this is a normal close; if the channel is
				 *  in the list, it means we're closing a cached channel (for which a permit
				 *  has already been released).
				 */
				synchronized (this.channelList) {
					if (this.channelList.contains(proxy)) {
						return;
					}
				}
				Semaphore checkoutPermits = CachingConnectionFactory.this.checkoutPermits.get(this.theConnection);
				if (checkoutPermits != null) {
					checkoutPermits.release();
					if (logger.isDebugEnabled()) {
						logger.debug("Released permit for '" + this.theConnection + "', remaining: "
							+ checkoutPermits.availablePermits());
					}
				}
				else {
					logger.error("LEAKAGE: No permits map entry for " + this.theConnection);
				}
			}
		}

		/**
		 * GUARDED by channelList.
		 * @param proxy the channel to close.
		 * @throws Exception an exception.
		 */
		private void logicalClose(ChannelProxy proxy) throws Exception {
			if (this.target == null) {
				return;
			}
			if (this.target != null && !this.target.isOpen()) {
				synchronized (this.targetMonitor) {
					if (this.target != null && !this.target.isOpen()) {
						if (this.target instanceof PublisherCallbackChannel) {
							this.target.close(); // emit nacks if necessary
						}
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
				setHighWaterMark();
			}
		}

		private void setHighWaterMark() {
			AtomicInteger hwm = CachingConnectionFactory.this.channelHighWaterMarks.get(this.channelListIdentity);
			if (hwm != null) {
				// No need for atomicity since we're sync'd on the channel list
				int prev = hwm.get();
				int size = this.channelList.size();
				if (size > prev) {
					hwm.set(size);
				}
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
					asyncClose();
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

		private void asyncClose() {
			ExecutorService executorService = (getExecutorService() != null
					? getExecutorService()
					: CachingConnectionFactory.this.deferredCloseExecutor);
			final Channel channel = CachedChannelInvocationHandler.this.target;
			executorService.execute(() -> {
				try {
					if (CachingConnectionFactory.this.publisherConfirms) {
						channel.waitForConfirmsOrDie(5000);
					}
					else {
						Thread.sleep(5000);
					}
				}
				catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
				}
				catch (Exception e2) { }
				finally {
					try {
						channel.close();
					}
					catch (IOException e3) { }
					catch (AlreadyClosedException e4) { }
					catch (TimeoutException e5) { }
					catch (ShutdownSignalException e6) {
						if (!RabbitUtils.isNormalShutdown(e6)) {
							logger.debug("Unexpected exception on deferred close", e6);
						}
					}
				}
			});
		}

	}

	private class ChannelCachingConnectionProxy implements ConnectionProxy { // NOSONAR - final (tests spy)

		private final AtomicBoolean closeNotified = new AtomicBoolean(false);

		private volatile Connection target;

		ChannelCachingConnectionProxy(Connection target) {
			this.target = target;
		}

		private Channel createBareChannel(boolean transactional) {
			Assert.state(this.target != null, "Can't create channel - no target connection.");
			return this.target.createChannel(transactional);
		}

		@Override
		public Channel createChannel(boolean transactional) {
			return getChannel(this, transactional);
		}

		@Override
		public void addBlockedListener(BlockedListener listener) {
			Assert.state(this.target != null, "Can't add blocked listener - no target connection.");
			this.target.addBlockedListener(listener);
		}

		@Override
		public boolean removeBlockedListener(BlockedListener listener) {
			Assert.state(this.target != null, "Can't remove blocked listener - no target connection.");
			return this.target.removeBlockedListener(listener);
		}

		@Override
		public void close() {
			if (CachingConnectionFactory.this.cacheMode == CacheMode.CONNECTION) {
				synchronized (CachingConnectionFactory.this.connectionMonitor) {
					/*
					 * Only connectionCacheSize open idle connections are allowed.
					 */
					if (!CachingConnectionFactory.this.idleConnections.contains(this)) {
						if (!isOpen()
								|| countOpenIdleConnections() >= CachingConnectionFactory.this.connectionCacheSize) {
							if (logger.isDebugEnabled()) {
								logger.debug("Completely closing connection '" + this + "'");
							}
							destroy();
						}
						if (logger.isDebugEnabled()) {
							logger.debug("Returning connection '" + this + "' to cache");
						}
						CachingConnectionFactory.this.idleConnections.add(this);
						if (CachingConnectionFactory.this.connectionHighWaterMark
								.get() < CachingConnectionFactory.this.idleConnections.size()) {
							CachingConnectionFactory.this.connectionHighWaterMark
									.set(CachingConnectionFactory.this.idleConnections.size());
						}
						CachingConnectionFactory.this.connectionMonitor.notifyAll();
					}
				}
			}
		}

		private int countOpenIdleConnections() {
			int n = 0;
			for (ChannelCachingConnectionProxy proxy : CachingConnectionFactory.this.idleConnections) {
				if (proxy.isOpen()) {
					n++;
				}
			}
			return n;
		}

		public void destroy() {
			if (CachingConnectionFactory.this.cacheMode == CacheMode.CHANNEL) {
				reset(CachingConnectionFactory.this.cachedChannelsNonTransactional,
						CachingConnectionFactory.this.cachedChannelsTransactional);
			}
			else {
				reset(CachingConnectionFactory.this.allocatedConnectionNonTransactionalChannels.get(this),
						CachingConnectionFactory.this.allocatedConnectionTransactionalChannels.get(this));
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
			return this.target != null && this.target.isOpen();
		}

		@Override
		public Connection getTargetConnection() {
			return this.target;
		}

		@Override
		public int getLocalPort() {
			Connection target = this.target; // NOSONAR (close)
			if (target != null) {
				return target.getLocalPort();
			}
			return 0;
		}

		@Override
		public String toString() {
			return "Proxy@" + ObjectUtils.getIdentityHexString(this) + " "
				+ (CachingConnectionFactory.this.cacheMode == CacheMode.CHANNEL ? "Shared " : "Dedicated ")
				+ "Rabbit Connection: " + this.target;
		}

	}

	/**
	 * Default implementation of {@link ConditionalExceptionLogger} for logging channel
	 * close exceptions.
	 * @since 1.5
	 */
	private static class DefaultChannelCloseLogger implements ConditionalExceptionLogger {

		DefaultChannelCloseLogger() {
			super();
		}

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
