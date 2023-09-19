/*
 * Copyright 2002-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.amqp.AmqpApplicationContextClosedException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.rabbit.support.ActiveObjectCounter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;

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
 * When the cache mode is {@link CacheMode#CONNECTION}, a new (or cached) connection is used for each
 * {@link #createConnection()};
 * connections are cached according to the {@link #setConnectionCacheSize(int) "connectionCacheSize" value}.
 * Both connections and channels are cached in this mode.
 * <p>
 * <b>{@link CacheMode#CONNECTION} is not compatible with a Rabbit Admin that auto-declares queues etc.</b>
 * <p>
 * <b>NOTE: This ConnectionFactory requires explicit closing of all Channels obtained from its Connection(s).</b>
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
 * @author Will Droste
 * @author Leonardo Ferreira
 */
@ManagedResource
public class CachingConnectionFactory extends AbstractConnectionFactory
		implements InitializingBean, ShutdownListener, SmartLifecycle {

	private static final String UNUSED = "unused";

	private static final int DEFAULT_CHANNEL_CACHE_SIZE = 25;

	private static final String DEFAULT_DEFERRED_POOL_PREFIX = "spring-rabbit-deferred-pool-";

	private static final int CHANNEL_EXEC_SHUTDOWN_TIMEOUT = 30;

	/**
	 * Create a unique ID for the pool.
	 */
	private static final AtomicInteger threadPoolId = new AtomicInteger(); // NOSONAR lower case

	private static final Set<String> txStarts = new HashSet<>(Arrays.asList("basicPublish", "basicAck", // NOSONAR
			"basicNack", "basicReject"));

	private static final Set<String> ackMethods = new HashSet<>(Arrays.asList("basicAck", // NOSONAR
			"basicNack", "basicReject"));

	private static final Set<String> txEnds = new HashSet<>(Arrays.asList("txCommit", "txRollback")); // NOSONAR

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

	/**
	 * The type of publisher confirms to use.
	 */
	public enum ConfirmType {

		/**
		 * Use {@code RabbitTemplate#waitForConfirms()} (or {@code waitForConfirmsOrDie()}
		 * within scoped operations.
		 */
		SIMPLE,

		/**
		 * Use with {@code CorrelationData} to correlate confirmations with sent
		 * messsages.
		 */
		CORRELATED,

		/**
		 * Publisher confirms are disabled (default).
		 */
		NONE

	}

	private final Set<ChannelCachingConnectionProxy> allocatedConnections = new HashSet<>();

	private final Map<ChannelCachingConnectionProxy, LinkedList<ChannelProxy>>
			allocatedConnectionNonTransactionalChannels = new HashMap<>();

	private final Map<ChannelCachingConnectionProxy, LinkedList<ChannelProxy>>
			allocatedConnectionTransactionalChannels = new HashMap<>();

	private final BlockingDeque<ChannelCachingConnectionProxy> idleConnections = new LinkedBlockingDeque<>();

	private final LinkedList<ChannelProxy> cachedChannelsNonTransactional = new LinkedList<>(); // NOSONAR removeFirst()

	private final LinkedList<ChannelProxy> cachedChannelsTransactional = new LinkedList<>(); // NOSONAR removeFirst()

	private final Map<Connection, Semaphore> checkoutPermits = new HashMap<>();

	private final Map<String, AtomicInteger> channelHighWaterMarks = new HashMap<>();

	private final AtomicInteger connectionHighWaterMark = new AtomicInteger();

	/** Synchronization monitor for the shared Connection. */
	private final Object connectionMonitor = new Object();

	private final ActiveObjectCounter<Channel> inFlightAsyncCloses = new ActiveObjectCounter<>();

	private final AtomicBoolean running = new AtomicBoolean();

	private long channelCheckoutTimeout = 0;

	private CacheMode cacheMode = CacheMode.CHANNEL;

	private int channelCacheSize = DEFAULT_CHANNEL_CACHE_SIZE;

	private int connectionCacheSize = 1;

	private int connectionLimit = Integer.MAX_VALUE;

	private ConfirmType confirmType = ConfirmType.NONE;

	private boolean publisherReturns;

	private PublisherCallbackChannelFactory publisherChannelFactory = PublisherCallbackChannelImpl.factory();

	private boolean defaultPublisherFactory = true;

	private volatile boolean active = true;

	private volatile boolean initialized;

	/**
	 * Executor used for channels if no explicit executor set.
	 */
	private volatile ExecutorService channelsExecutor;

	private volatile boolean stopped;

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
	public CachingConnectionFactory(@Nullable String hostname) {
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
	 * @param hostNameArg the host name to connect to
	 * @param port the port number
	 */
	public CachingConnectionFactory(@Nullable String hostNameArg, int port) {
		super(newRabbitConnectionFactory());
		String hostname = hostNameArg;
		if (!StringUtils.hasText(hostname)) {
			hostname = getDefaultHostName();
		}
		setHost(hostname);
		setPort(port);
		doSetPublisherConnectionFactory(new CachingConnectionFactory(getRabbitConnectionFactory(), true));
	}

	/**
	 * Create a new CachingConnectionFactory given a {@link URI}.
	 * @param uri the amqp uri configuring the connection
	 * @since 1.5
	 */
	public CachingConnectionFactory(URI uri) {
		super(newRabbitConnectionFactory());
		setUri(uri);
		doSetPublisherConnectionFactory(new CachingConnectionFactory(getRabbitConnectionFactory(), true));
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
				rabbitConnectionFactory.setAutomaticRecoveryEnabled(false);
				logger.warn("***\nAutomatic Recovery was Enabled in the provided connection factory;\n"
						+ "while Spring AMQP is generally compatible with this feature, there\n"
						+ "are some corner cases where problems arise. Spring AMQP\n"
						+ "prefers to use its own recovery mechanisms; when this option is true, you may receive\n"
						+ "'AutoRecoverConnectionNotCurrentlyOpenException's until the connection is recovered.\n"
						+ "It has therefore been disabled; if you really wish to enable it, use\n"
						+ "'getRabbitConnectionFactory().setAutomaticRecoveryEnabled(true)',\n"
						+ "but this is discouraged.");
			}
			super.setPublisherConnectionFactory(new CachingConnectionFactory(getRabbitConnectionFactory(), true));
		}
		else {
			super.setPublisherConnectionFactory(null);
			this.defaultPublisherFactory = false;
		}
	}

	private static com.rabbitmq.client.ConnectionFactory newRabbitConnectionFactory() {
		com.rabbitmq.client.ConnectionFactory connectionFactory = new com.rabbitmq.client.ConnectionFactory();
		connectionFactory.setAutomaticRecoveryEnabled(false);
		return connectionFactory;
	}

	@Override
	public void setPublisherConnectionFactory(@Nullable AbstractConnectionFactory publisherConnectionFactory) {
		super.setPublisherConnectionFactory(publisherConnectionFactory);
		this.defaultPublisherFactory = false;
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
		if (this.defaultPublisherFactory) {
			((CachingConnectionFactory) getPublisherConnectionFactory()).setChannelCacheSize(sessionCacheSize); // NOSONAR
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
		if (this.defaultPublisherFactory) {
			((CachingConnectionFactory) getPublisherConnectionFactory()).setCacheMode(cacheMode); // NOSONAR
		}
	}

	public int getConnectionCacheSize() {
		return this.connectionCacheSize;
	}

	public void setConnectionCacheSize(int connectionCacheSize) {
		Assert.isTrue(connectionCacheSize >= 1, "Connection cache size must be 1 or higher.");
		this.connectionCacheSize = connectionCacheSize;
		if (this.defaultPublisherFactory) {
			((CachingConnectionFactory) getPublisherConnectionFactory()).setConnectionCacheSize(connectionCacheSize); // NOSONAR
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
		if (this.defaultPublisherFactory) {
			((CachingConnectionFactory) getPublisherConnectionFactory()).setConnectionLimit(connectionLimit); // NOSONAR
		}
	}

	@Override
	public boolean isPublisherConfirms() {
		return ConfirmType.CORRELATED.equals(this.confirmType);
	}

	@Override
	public boolean isPublisherReturns() {
		return this.publisherReturns;
	}

	public void setPublisherReturns(boolean publisherReturns) {
		this.publisherReturns = publisherReturns;
		if (this.defaultPublisherFactory) {
			((CachingConnectionFactory) getPublisherConnectionFactory()).setPublisherReturns(publisherReturns); // NOSONAR
		}
	}

	@Override
	public boolean isSimplePublisherConfirms() {
		return this.confirmType.equals(ConfirmType.SIMPLE);
	}

	/**
	 * Set the confirm type to use; default {@link ConfirmType#NONE}.
	 * @param confirmType the confirm type.
	 * @since 2.2
	 */
	public void setPublisherConfirmType(ConfirmType confirmType) {
		Assert.notNull(confirmType, "'confirmType' cannot be null");
		this.confirmType = confirmType;
		if (this.defaultPublisherFactory) {
			((CachingConnectionFactory) getPublisherConnectionFactory()).setPublisherConfirmType(confirmType); // NOSONAR
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
		if (this.defaultPublisherFactory) {
			((CachingConnectionFactory) getPublisherConnectionFactory())
					.setChannelCheckoutTimeout(channelCheckoutTimeout); // NOSONAR
		}
	}

	/**
	 * Set the factory to use to create {@link PublisherCallbackChannel} instances.
	 * @param publisherChannelFactory the factory.
	 * @since 2.1.6
	 */
	public void setPublisherChannelFactory(PublisherCallbackChannelFactory publisherChannelFactory) {
		Assert.notNull(publisherChannelFactory, "'publisherChannelFactory' cannot be null");
		this.publisherChannelFactory = publisherChannelFactory;
	}

	@Override
	public int getPhase() {
		return Integer.MIN_VALUE;
	}

	@Override
	public void afterPropertiesSet() {
		this.initialized = true;
		if (this.cacheMode == CacheMode.CHANNEL) {
			Assert.isTrue(this.connectionCacheSize == 1,
					"When the cache mode is 'CHANNEL', the connection cache size cannot be configured.");
		}
		initCacheWaterMarks();
		if (this.defaultPublisherFactory) {
			((CachingConnectionFactory) getPublisherConnectionFactory()).afterPropertiesSet(); // NOSONAR
		}
	}

	@Override
	public void start() {
		this.running.set(true);
	}

	@Override
	public void stop() {
		this.running.set(false);
		resetConnection();
	}

	@Override
	public boolean isRunning() {
		return this.running.get();
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

	private Channel getChannel(ChannelCachingConnectionProxy connection, boolean transactional) {
		Semaphore permits = null;
		if (this.channelCheckoutTimeout > 0) {
			permits = obtainPermits(connection);
		}
		LinkedList<ChannelProxy> channelList = determineChannelList(connection, transactional);
		ChannelProxy channel = null;
		if (connection.isOpen()) {
			channel = findOpenChannel(channelList);
			if (channel != null && logger.isTraceEnabled()) {
				logger.trace("Found cached Rabbit Channel: " + channel.toString());
			}
		}
		if (channel == null) {
			try {
				channel = getCachedChannelProxy(connection, channelList, transactional);
			}
			catch (RuntimeException e) {
				if (permits != null) {
					permits.release();
					if (logger.isDebugEnabled()) {
						logger.debug("Could not get channel; released permit for " + connection + ", remaining:"
								+ permits.availablePermits());
					}
				}
				throw e;
			}
		}
		return channel;
	}

	private Semaphore obtainPermits(ChannelCachingConnectionProxy connection) {
		Semaphore permits;
		permits = this.checkoutPermits.get(connection);
		if (permits != null) {
			try {
				if (!permits.tryAcquire(this.channelCheckoutTimeout, TimeUnit.MILLISECONDS)) {
					throw new AmqpTimeoutException("No available channels");
				}
				if (logger.isDebugEnabled()) {
					logger.debug(
							"Acquired permit for " + connection + ", remaining:" + permits.availablePermits());
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
		return permits;
	}

	@Nullable
	private ChannelProxy findOpenChannel(LinkedList<ChannelProxy> channelList) { // NOSONAR - LL Vs. L - removeFirst()

		ChannelProxy channel = null;
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
					cleanUpClosedChannel(channel);
					channel = null;
				}
			}
		}
		return channel;
	}

	private void cleanUpClosedChannel(ChannelProxy channel) {
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
	}

	private LinkedList<ChannelProxy> determineChannelList(ChannelCachingConnectionProxy connection, // NOSONAR LL
			boolean transactional) {
		LinkedList<ChannelProxy> channelList; // NOSONAR must be LinkedList
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
		return channelList;
	}

	private ChannelProxy getCachedChannelProxy(ChannelCachingConnectionProxy connection,
			LinkedList<ChannelProxy> channelList, boolean transactional) { //NOSONAR LinkedList for addLast()

		Channel targetChannel = createBareChannel(connection, transactional);
		if (logger.isDebugEnabled()) {
			logger.debug("Creating cached Rabbit Channel from " + targetChannel);
		}
		getChannelListener().onCreate(targetChannel, transactional);
		Class<?>[] interfaces;
		if (ConfirmType.CORRELATED.equals(this.confirmType) || this.publisherReturns) {
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
		return null; // NOSONAR doCreate will throw an exception
	}

	private Channel doCreateBareChannel(ChannelCachingConnectionProxy conn, boolean transactional) {
		Channel channel = conn.createBareChannel(transactional);
		if (!ConfirmType.NONE.equals(this.confirmType)) {
			try {
				channel.confirmSelect();
			}
			catch (IOException e) {
				logger.error("Could not configure the channel to receive publisher confirms", e);
			}
		}
		if ((ConfirmType.CORRELATED.equals(this.confirmType) || this.publisherReturns)
				&& !(channel instanceof PublisherCallbackChannelImpl)) {
			channel = this.publisherChannelFactory.createChannel(channel, getChannelsExecutor());
		}
		if (channel != null) {
			channel.addShutdownListener(this);
		}
		return channel; // NOSONAR - Simple connection throws exception
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
				return connectionFromCache();
			}
		}
		return null; // NOSONAR - never reach here - exceptions
	}

	private Connection connectionFromCache() {
		ChannelCachingConnectionProxy cachedConnection = findIdleConnection();
		long now = System.currentTimeMillis();
		if (cachedConnection == null && countOpenConnections() >= this.connectionLimit) {
			cachedConnection = waitForConnection(now);
		}
		if (cachedConnection == null) {
			if (countOpenConnections() >= this.connectionLimit
					&& System.currentTimeMillis() - now >= this.channelCheckoutTimeout) {
				throw new AmqpTimeoutException("Timed out attempting to get a connection");
			}
			cachedConnection = new ChannelCachingConnectionProxy(super.createBareConnection());
			if (logger.isDebugEnabled()) {
				logger.debug("Adding new connection '" + cachedConnection + "'");
			}
			this.allocatedConnections.add(cachedConnection);
			this.allocatedConnectionNonTransactionalChannels.put(cachedConnection, new LinkedList<ChannelProxy>());
			this.channelHighWaterMarks.put(ObjectUtils.getIdentityHexString(
					this.allocatedConnectionNonTransactionalChannels.get(cachedConnection)), new AtomicInteger());
			this.allocatedConnectionTransactionalChannels.put(cachedConnection, new LinkedList<ChannelProxy>());
			this.channelHighWaterMarks.put(
					ObjectUtils
							.getIdentityHexString(this.allocatedConnectionTransactionalChannels.get(cachedConnection)),
					new AtomicInteger());
			this.checkoutPermits.put(cachedConnection, new Semaphore(this.channelCacheSize));
			getConnectionListener().onCreate(cachedConnection);
		}
		else if (!cachedConnection.isOpen()) {
			try {
				refreshProxyConnection(cachedConnection);
			}
			catch (Exception e) {
				this.idleConnections.addLast(cachedConnection);
			}
		}
		else {
			if (logger.isDebugEnabled()) {
				logger.debug("Obtained connection '" + cachedConnection + "' from cache");
			}
		}
		return cachedConnection;
	}

	@Nullable
	private ChannelCachingConnectionProxy waitForConnection(long now) {
		ChannelCachingConnectionProxy cachedConnection = null;
		while (cachedConnection == null && System.currentTimeMillis() - now < this.channelCheckoutTimeout) {
			if (countOpenConnections() >= this.connectionLimit) {
				try {
					this.connectionMonitor.wait(this.channelCheckoutTimeout);
					cachedConnection = findIdleConnection();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new AmqpException("Interrupted while waiting for a connection", e);
				}
			}
		}
		return cachedConnection;
	}

	/*
	 * Iterate over the idle connections looking for an open one. If there are no idle,
	 * return null, if there are no open idle, return the first closed idle so it can
	 * be reopened.
	 */
	@Nullable
	private ChannelCachingConnectionProxy findIdleConnection() {
		ChannelCachingConnectionProxy cachedConnection = null;
		ChannelCachingConnectionProxy lastIdle = this.idleConnections.peekLast();
		while (cachedConnection == null) {
			cachedConnection = this.idleConnections.poll();
			if (cachedConnection != null) {
				if (!cachedConnection.isOpen()) {
					if (logger.isDebugEnabled()) {
						logger.debug("Skipping closed connection '" + cachedConnection + "'");
					}
					cachedConnection.notifyCloseIfNecessary();
					this.idleConnections.addLast(cachedConnection);
					if (cachedConnection.equals(lastIdle)) {
						// all of the idle connections are closed.
						cachedConnection = this.idleConnections.poll();
						break;
					}
					cachedConnection = null;
				}
			}
			else {
				break;
			}
		}
		return cachedConnection;
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
			synchronized (this.connectionMonitor) {
				if (this.channelsExecutor != null) {
					try {
						if (!this.inFlightAsyncCloses.await(CHANNEL_EXEC_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
							this.logger
									.warn("Async closes are still in-flight: " + this.inFlightAsyncCloses.getCount());
						}
						this.channelsExecutor.shutdown();
						if (!this.channelsExecutor.awaitTermination(CHANNEL_EXEC_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
							this.logger.warn("Channel executor failed to shut down");
						}
					}
					catch (@SuppressWarnings(UNUSED) InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					finally {
						this.channelsExecutor = null;
					}
				}
			}
		}
	}

	/**
	 * Close the connection(s). This will impact any in-process operations. New
	 * connection(s) will be created on demand after this method returns. This might be
	 * used to force a reconnect to the primary broker after failing over to a secondary
	 * broker.
	 */
	@Override
	public void resetConnection() {
		synchronized (this.connectionMonitor) {
			if (this.connection.target != null) {
				this.connection.destroy();
			}
			this.allocatedConnections.forEach(c -> c.destroy());
			this.channelHighWaterMarks.values().forEach(count -> count.set(0));
			this.connectionHighWaterMark.set(0);
		}
		if (this.defaultPublisherFactory) {
			((CachingConnectionFactory) getPublisherConnectionFactory()).resetConnection(); // NOSONAR
		}
	}

	/*
	 * Reset the Channel cache and underlying shared Connection, to be reinitialized on next access.
	 */
	protected void reset(List<ChannelProxy> channels, List<ChannelProxy> txChannels,
			Map<Channel, ChannelProxy> channelsAwaitingAcks) {

		this.active = false;
		closeAndClear(channels);
		closeAndClear(txChannels);
		closeChannels(channelsAwaitingAcks.values());
		channelsAwaitingAcks.clear();
		this.active = true;
	}

	protected void closeAndClear(Collection<ChannelProxy> theChannels) {
		synchronized (theChannels) {
			closeChannels(theChannels);
			theChannels.clear();
		}
	}

	protected void closeChannels(Collection<ChannelProxy> theChannels) {
		for (ChannelProxy channel : theChannels) {
			try {
				channel.close();
			}
			catch (Exception ex) {
				logger.trace("Could not close cached Rabbit Channel", ex);
			}
		}
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
				props.setProperty("idleConnectionsHighWater", Integer.toString(this.connectionHighWaterMark.get()));
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
		if (this.defaultPublisherFactory) {
			return ((CachingConnectionFactory) getPublisherConnectionFactory()).getCacheProperties(); // NOSONAR
		}
		return new Properties();
	}

	private void putConnectionName(Properties props, ConnectionProxy connection, String keySuffix) {
		Connection targetConnection = connection.getTargetConnection(); // NOSONAR (close())
		if (targetConnection != null) {
			com.rabbitmq.client.Connection delegate = targetConnection.getDelegate();
			if (delegate != null) {
				String name = delegate.getClientProvidedName();
				if (name != null) {
					props.put("connectionName" + keySuffix, name);
				}
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

	/**
	 * Determine the executor service used for target channels.
	 * @return specified executor service otherwise the default one is created and returned.
	 * @since 1.7.9
	 */
	protected ExecutorService getChannelsExecutor() {
		if (getExecutorService() != null) {
			return getExecutorService(); // NOSONAR never null
		}
		if (this.channelsExecutor == null) {
			synchronized (this.connectionMonitor) {
				if (this.channelsExecutor == null) {
					final String threadPrefix =
							getBeanName() == null
									? DEFAULT_DEFERRED_POOL_PREFIX + threadPoolId.incrementAndGet()
									: getBeanName();
					ThreadFactory threadPoolFactory = new CustomizableThreadFactory(threadPrefix); // NOSONAR never null
					this.channelsExecutor = Executors.newCachedThreadPool(threadPoolFactory);
				}
			}
		}
		return this.channelsExecutor;
	}

	@Override
	public String toString() {
		String host = getHost();
		int port = getPort();
		List<Address> addresses = null;
		try {
			addresses = getAddresses();
		}
		catch (IOException ex) {
			host = "AddressResolver threw exception: " + ex.getMessage();
		}
		return "CachingConnectionFactory [channelCacheSize=" + this.channelCacheSize
				+ (addresses != null
					? ", addresses=" + addresses
					: (host != null ? ", host=" + host : "")
						+ (port > 0 ? ", port=" + port : ""))
				+ ", active=" + this.active
				+ " " + super.toString() + "]";
	}

	private final class CachedChannelInvocationHandler implements InvocationHandler {

		private static final int ASYNC_CLOSE_TIMEOUT = 5_000;

		private final ChannelCachingConnectionProxy theConnection;

		private final LinkedList<ChannelProxy> channelList; // NOSONAR addLast()

		private final String channelListIdentity;

		private final Object targetMonitor = new Object();

		private final boolean transactional;

		private final boolean confirmSelected = ConfirmType.SIMPLE.equals(CachingConnectionFactory.this.confirmType);

		private final boolean publisherConfirms =
				ConfirmType.CORRELATED.equals(CachingConnectionFactory.this.confirmType);

		private volatile Channel target;

		private volatile boolean txStarted;

		CachedChannelInvocationHandler(ChannelCachingConnectionProxy connection,
				Channel target,
				LinkedList<ChannelProxy> channelList, // NOSONAR addLast()
				boolean transactional) {

			this.theConnection = connection;
			this.target = target;
			this.channelList = channelList;
			this.channelListIdentity = ObjectUtils.getIdentityHexString(channelList);
			this.transactional = transactional;
		}

		@Override // NOSONAR complexity
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable { // NOSONAR NCSS lines
			if (logger.isTraceEnabled() && !method.getName().equals("toString")
					&& !method.getName().equals("hashCode") && !method.getName().equals("equals")) {
				try {
					logger.trace(this.target + " channel." + method.getName() + "("
							+ (args != null ? Arrays.toString(args) : "") + ")");
				}
				catch (Exception e) {
					// empty - some mocks fail here
				}
			}
			String methodName = method.getName();
			if (methodName.equals("txSelect") && !this.transactional) {
				throw new UnsupportedOperationException("Cannot start transaction on non-transactional channel");
			}
			if (methodName.equals("equals")) {
				// Only consider equal when proxies are identical.
				return (proxy == args[0]); // NOSONAR
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
				if (CachingConnectionFactory.this.active && !RabbitUtils.isPhysicalCloseRequired()) {
					logicalClose((ChannelProxy) proxy);
					return null;
				}
				else {
					physicalClose(proxy);
					return null;
				}
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
			else if (methodName.equals("isConfirmSelected")) {
				return this.confirmSelected;
			}
			else if (methodName.equals("isPublisherConfirms")) {
				return this.publisherConfirms;
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
				Semaphore permits = CachingConnectionFactory.this.checkoutPermits.get(this.theConnection);
				if (permits != null) {
					permits.release();
					if (logger.isDebugEnabled()) {
						logger.debug("Released permit for '" + this.theConnection + "', remaining: "
								+ permits.availablePermits());
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
		 * @throws TimeoutException time out on close.
		 * @throws IOException  exception on close.
		 */
		private void logicalClose(ChannelProxy proxy) throws IOException, TimeoutException {
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
						else {
							releasePermitIfNecessary(proxy);
						}
						this.target = null;
						return;
					}
				}
			}
			returnToCache(proxy);
		}

		private void returnToCache(ChannelProxy proxy) {
			if (CachingConnectionFactory.this.active && this.publisherConfirms
					&& proxy instanceof PublisherCallbackChannel) {

				this.theConnection.channelsAwaitingAcks.put(this.target, proxy);
				((PublisherCallbackChannel) proxy)
						.setAfterAckCallback(c ->
								doReturnToCache(this.theConnection.channelsAwaitingAcks.remove(c)));
			}
			else {
				doReturnToCache(proxy);
			}
		}

		private void doReturnToCache(Channel proxy) {
			if (proxy != null) {
				synchronized (this.channelList) {
					// Allow for multiple close calls...
					if (CachingConnectionFactory.this.active) {
						cacheOrClose(proxy);
					}
					else {
						if (proxy.isOpen()) {
							try {
								physicalClose(proxy);
							}
							catch (@SuppressWarnings(UNUSED) Exception e) {
							}
						}
					}
				}
			}
		}

		private void cacheOrClose(Channel proxy) {
			boolean alreadyCached = this.channelList.contains(proxy);
			if (this.channelList.size() >= getChannelCacheSize() && !alreadyCached) {
				if (logger.isTraceEnabled()) {
					logger.trace("Cache limit reached: " + this.target);
				}
				try {
					physicalClose(proxy);
				}
				catch (@SuppressWarnings(UNUSED) Exception e) {
				}
			}
			else if (!alreadyCached) {
				if (logger.isTraceEnabled()) {
					logger.trace("Returning cached Channel: " + this.target);
				}
				releasePermitIfNecessary(proxy);
				this.channelList.addLast((ChannelProxy) proxy);
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

		private void physicalClose(Object proxy) throws IOException, TimeoutException {
			if (logger.isDebugEnabled()) {
				logger.debug("Closing cached Channel: " + this.target);
			}
			RabbitUtils.clearPhysicalCloseRequired();
			if (this.target == null) {
				return;
			}
			boolean async = false;
			try {
				if (CachingConnectionFactory.this.active &&
						(ConfirmType.CORRELATED.equals(CachingConnectionFactory.this.confirmType) ||
								CachingConnectionFactory.this.publisherReturns)) {
					async = true;
					asyncClose(proxy);
				}
				else {
					this.target.close();
					if (this.target instanceof AutorecoveringChannel auto) {
						ClosingRecoveryListener.removeChannel(auto);
					}
				}
			}
			catch (AlreadyClosedException e) {
				if (logger.isTraceEnabled()) {
					logger.trace(this.target + " is already closed", e);
				}
			}
			finally {
				this.target = null;
				if (!async) {
					releasePermitIfNecessary(proxy);
				}
			}
		}

		private void asyncClose(Object proxy) {
			ExecutorService executorService = getChannelsExecutor();
			final Channel channel = CachedChannelInvocationHandler.this.target;
			CachingConnectionFactory.this.inFlightAsyncCloses.add(channel);
			try {
				executorService.execute(() -> {
					try {
						if (ConfirmType.CORRELATED.equals(CachingConnectionFactory.this.confirmType)) {
							channel.waitForConfirmsOrDie(ASYNC_CLOSE_TIMEOUT);
						}
						else {
							Thread.sleep(ASYNC_CLOSE_TIMEOUT);
						}
					}
					catch (@SuppressWarnings(UNUSED) InterruptedException e1) {
						Thread.currentThread().interrupt();
					}
					catch (@SuppressWarnings(UNUSED) Exception e2) {
					}
					finally {
						try {
							channel.close();
						}
						catch (@SuppressWarnings(UNUSED) IOException e3) {
						}
						catch (@SuppressWarnings(UNUSED) AlreadyClosedException e4) {
						}
						catch (@SuppressWarnings(UNUSED) TimeoutException e5) {
						}
						catch (ShutdownSignalException e6) {
							if (!RabbitUtils.isNormalShutdown(e6)) {
								logger.debug("Unexpected exception on deferred close", e6);
							}
						}
						finally {
							CachingConnectionFactory.this.inFlightAsyncCloses.release(channel);
							releasePermitIfNecessary(proxy);
						}
					}
				});
			}
			catch (@SuppressWarnings(UNUSED) RuntimeException e) {
				CachingConnectionFactory.this.inFlightAsyncCloses.release(channel);
			}
		}

	}

	private class ChannelCachingConnectionProxy implements ConnectionProxy { // NOSONAR - final (tests spy)

		private final AtomicBoolean closeNotified = new AtomicBoolean(false);

		private final ConcurrentMap<Channel, ChannelProxy> channelsAwaitingAcks = new ConcurrentHashMap<>();

		private volatile Connection target;

		ChannelCachingConnectionProxy(@Nullable Connection target) {
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
						CachingConnectionFactory.this.cachedChannelsTransactional, this.channelsAwaitingAcks);
			}
			else {
				reset(CachingConnectionFactory.this.allocatedConnectionNonTransactionalChannels.get(this),
						CachingConnectionFactory.this.allocatedConnectionTransactionalChannels.get(this),
						this.channelsAwaitingAcks);
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
		public com.rabbitmq.client.Connection getDelegate() {
			return this.target.getDelegate();
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

}
