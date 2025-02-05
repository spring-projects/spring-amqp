/*
 * Copyright 2002-2025 the original author or authors.
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
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.amqp.support.ConditionalExceptionLogger;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.backoff.BackOff;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Steve Powell
 * @author Artem Bilan
 * @author Will Droste
 * @author Christian Tzolov
 * @author Salk Lee
 *
 */
public abstract class AbstractConnectionFactory implements ConnectionFactory, DisposableBean, BeanNameAware,
		ApplicationContextAware, ApplicationEventPublisherAware, ApplicationListener<ContextClosedEvent>,
		ShutdownListener {

	/**
	 * The mode used to shuffle the addresses.
	 */
	public enum AddressShuffleMode {

		/**
		 * Do not shuffle the addresses before or after opening a connection; attempt connections in a fixed order.
		 */
		NONE,

		/**
		 * Randomly shuffle the addresses before opening a connection; attempt connections in the new order.
		 */
		RANDOM,

		/**
		 * Shuffle the addresses after opening a connection, moving the first address to the end.
		 */
		INORDER

	}

	private static final String PUBLISHER_SUFFIX = ".publisher";

	public static final int DEFAULT_CLOSE_TIMEOUT = 30000;

	private static final String BAD_URI = "setUri() was passed an invalid URI; it is ignored";

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	private final Lock lock = new ReentrantLock();

	private final com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory;

	private final CompositeConnectionListener connectionListener = new CompositeConnectionListener();

	private final CompositeChannelListener channelListener = new CompositeChannelListener();

	private final AtomicInteger defaultConnectionNameStrategyCounter = new AtomicInteger();

	private ConditionalExceptionLogger closeExceptionLogger = new DefaultChannelCloseLogger();

	private @Nullable AbstractConnectionFactory publisherConnectionFactory;

	private RecoveryListener recoveryListener = new RecoveryListener() {

		@Override
		public void handleRecoveryStarted(Recoverable recoverable) {
			if (AbstractConnectionFactory.this.logger.isDebugEnabled()) {
				AbstractConnectionFactory.this.logger.debug("Connection recovery started: " + recoverable);
			}
		}

		@Override
		public void handleRecovery(Recoverable recoverable) {
			if (AbstractConnectionFactory.this.logger.isDebugEnabled()) {
				AbstractConnectionFactory.this.logger.debug("Connection recovery complete: " + recoverable);
			}
		}

	};

	private @Nullable ExecutorService executorService;

	private @Nullable List<Address> addresses;

	private AddressShuffleMode addressShuffleMode = AddressShuffleMode.RANDOM;

	private int closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	private ConnectionNameStrategy connectionNameStrategy = connectionFactory -> (this.beanName != null ? this.beanName
			: "SpringAMQP") +
			"#" + ObjectUtils.getIdentityHexString(this) + ":" +
			this.defaultConnectionNameStrategyCounter.getAndIncrement();

	private @Nullable String beanName;

	@SuppressWarnings("NullAway.Init")
	private ApplicationContext applicationContext;

	private @Nullable ApplicationEventPublisher applicationEventPublisher;

	private @Nullable AddressResolver addressResolver;

	private volatile boolean contextStopped;

	private @Nullable BackOff connectionCreatingBackOff;

	/**
	 * Create a new AbstractConnectionFactory for the given target ConnectionFactory, with no publisher connection
	 * factory.
	 * @param rabbitConnectionFactory the target ConnectionFactory
	 */
	public AbstractConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory) {
		Assert.notNull(rabbitConnectionFactory, "Target ConnectionFactory must not be null");
		this.rabbitConnectionFactory = rabbitConnectionFactory;
	}

	/**
	 * Set a custom publisher connection factory; the type does not need to be the same as this factory.
	 * @param publisherConnectionFactory the factory.
	 * @since 2.3.2
	 */
	public void setPublisherConnectionFactory(
			@Nullable AbstractConnectionFactory publisherConnectionFactory) {

		doSetPublisherConnectionFactory(publisherConnectionFactory);
	}

	protected final void doSetPublisherConnectionFactory(
			@Nullable AbstractConnectionFactory publisherConnectionFactory) {

		this.publisherConnectionFactory = publisherConnectionFactory;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setApplicationContext(applicationContext);
		}
	}

	protected ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setApplicationEventPublisher(applicationEventPublisher);
		}
	}

	protected @Nullable ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}

	@Override
	public void onApplicationEvent(ContextClosedEvent event) {
		if (event.getApplicationContext().equals(getApplicationContext())) {
			this.contextStopped = true;
		}
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.onApplicationEvent(event);
		}
	}

	protected boolean getContextStopped() {
		return this.contextStopped;
	}

	/**
	 * Return a reference to the underlying Rabbit Connection factory.
	 * @return the connection factory.
	 * @since 1.5.6
	 */
	public com.rabbitmq.client.ConnectionFactory getRabbitConnectionFactory() {
		return this.rabbitConnectionFactory;
	}

	/**
	 * Return the username from the underlying rabbit connection factory.
	 * @return the username.
	 * @since 1.6
	 */
	@Override
	public String getUsername() {
		return this.rabbitConnectionFactory.getUsername();
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

	/**
	 * Set the {@link ThreadFactory} on the underlying rabbit connection factory.
	 * @param threadFactory the thread factory.
	 * @since 1.5.3
	 */
	public void setConnectionThreadFactory(ThreadFactory threadFactory) {
		this.rabbitConnectionFactory.setThreadFactory(threadFactory);
	}

	/**
	 * Set an {@link AddressResolver} to use when creating connections; overrides {@link #setAddresses(String)},
	 * {@link #setHost(String)}, and {@link #setPort(int)}.
	 * @param addressResolver the resolver.
	 * @since 2.1.15
	 */
	public void setAddressResolver(AddressResolver addressResolver) {
		this.addressResolver = addressResolver; // NOSONAR - sync inconsistency
	}

	/**
	 * @param uri the URI
	 * @since 1.5
	 * @see com.rabbitmq.client.ConnectionFactory#setUri(URI)
	 */
	public void setUri(URI uri) {
		try {
			this.rabbitConnectionFactory.setUri(uri);
		}
		catch (URISyntaxException | GeneralSecurityException use) {
			this.logger.info(BAD_URI, use);
		}
	}

	/**
	 * @param uri the URI
	 * @since 1.5
	 * @see com.rabbitmq.client.ConnectionFactory#setUri(String)
	 */
	public void setUri(String uri) {
		try {
			this.rabbitConnectionFactory.setUri(uri);
		}
		catch (URISyntaxException | GeneralSecurityException use) {
			this.logger.info(BAD_URI, use);
		}
	}

	@Override
	@Nullable
	public String getHost() {
		return this.rabbitConnectionFactory.getHost();
	}

	public void setVirtualHost(String virtualHost) {
		this.rabbitConnectionFactory.setVirtualHost(virtualHost);
	}

	@Override
	public String getVirtualHost() {
		return this.rabbitConnectionFactory.getVirtualHost();
	}

	public void setPort(int port) {
		this.rabbitConnectionFactory.setPort(port);
	}

	public void setRequestedHeartBeat(int requestedHeartBeat) {
		this.rabbitConnectionFactory.setRequestedHeartbeat(requestedHeartBeat);
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.rabbitConnectionFactory.setConnectionTimeout(connectionTimeout);
	}

	@Override
	public int getPort() {
		return this.rabbitConnectionFactory.getPort();
	}

	/**
	 * Set addresses for clustering. This property overrides the host+port properties if not empty.
	 * @param addresses list of addresses in form {@code host[:port]}.
	 * @since 3.2.1
	 */
	public void setAddresses(List<String> addresses) {
		Assert.notEmpty(addresses, "Addresses must not be empty");
		setAddresses(String.join(",", addresses));
	}
	/**
	 * Set addresses for clustering. This property overrides the host+port properties if not empty.
	 * @param addresses list of addresses with form {@code host1[:port1],host2[:port2],...}.
	 */
	public void setAddresses(String addresses) {
		this.lock.lock();
		try {
			if (StringUtils.hasText(addresses)) {
				Address[] addressArray = Address.parseAddresses(addresses);
				if (addressArray.length > 0) {
					this.addresses = new LinkedList<>(Arrays.asList(addressArray));
					if (this.publisherConnectionFactory != null) {
						this.publisherConnectionFactory.setAddresses(addresses);
					}
					return;
				}
			}
			this.logger.info("setAddresses() called with an empty value, will be using the host+port "
					+ " or addressResolver properties for connections");
			this.addresses = null;
		}
		finally {
			this.lock.unlock();
		}
	}

	protected @Nullable List<Address> getAddresses() throws IOException {
		this.lock.lock();
		try {
			return this.addressResolver != null ? this.addressResolver.getAddresses() : this.addresses;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * A composite connection listener to be used by subclasses when creating and closing connections.
	 *
	 * @return the connection listener
	 */
	protected ConnectionListener getConnectionListener() {
		return this.connectionListener;
	}

	/**
	 * A composite channel listener to be used by subclasses when creating and closing channels.
	 *
	 * @return the channel listener
	 */
	protected ChannelListener getChannelListener() {
		return this.channelListener;
	}

	public void setConnectionListeners(List<? extends ConnectionListener> listeners) {
		this.connectionListener.setDelegates(listeners);
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setConnectionListeners(listeners);
		}
	}

	@Override
	public void addConnectionListener(ConnectionListener listener) {
		this.connectionListener.addDelegate(listener);
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.addConnectionListener(listener);
		}
	}

	@Override
	public boolean removeConnectionListener(ConnectionListener listener) {
		boolean result = this.connectionListener.removeDelegate(listener);
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.removeConnectionListener(listener); // NOSONAR
		}
		return result;
	}

	@Override
	public void clearConnectionListeners() {
		this.connectionListener.clearDelegates();
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.clearConnectionListeners();
		}
	}

	public void setChannelListeners(List<? extends ChannelListener> listeners) {
		this.channelListener.setDelegates(listeners);
	}

	/**
	 * Set a {@link RecoveryListener} that will be added to each connection created.
	 * @param recoveryListener the listener.
	 * @since 2.0
	 */
	public void setRecoveryListener(RecoveryListener recoveryListener) {
		this.recoveryListener = recoveryListener;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setRecoveryListener(recoveryListener);
		}
	}

	public void addChannelListener(ChannelListener listener) {
		this.channelListener.addDelegate(listener);
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.addChannelListener(listener);
		}
	}

	/**
	 * Provide an Executor for use by the Rabbit ConnectionFactory when creating connections. Can either be an
	 * ExecutorService or a Spring ThreadPoolTaskExecutor, as defined by a &lt;task:executor/&gt; element.
	 * @param executor The executor.
	 */
	public void setExecutor(Executor executor) {
		boolean isExecutorService = executor instanceof ExecutorService;
		boolean isThreadPoolTaskExecutor = executor instanceof ThreadPoolTaskExecutor;
		Assert.isTrue(isExecutorService || isThreadPoolTaskExecutor,
				"'executor' must be an 'ExecutorService' or a 'ThreadPoolTaskExecutor'");
		if (isExecutorService) {
			this.executorService = (ExecutorService) executor;
		}
		else {
			this.executorService = ((ThreadPoolTaskExecutor) executor).getThreadPoolExecutor();
		}
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setExecutor(executor);
		}
	}

	protected @Nullable ExecutorService getExecutorService() {
		return this.executorService;
	}

	/**
	 * How long to wait (milliseconds) for a response to a connection close operation from the broker;
	 * default 30000 (30 seconds).
	 * Also used for {@link com.rabbitmq.client.Channel#waitForConfirms()}.
	 * @param closeTimeout the closeTimeout to set.
	 */
	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = closeTimeout;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setCloseTimeout(closeTimeout);
		}
	}

	public int getCloseTimeout() {
		return this.closeTimeout;
	}

	/**
	 * Provide a {@link ConnectionNameStrategy} to build the name for the target RabbitMQ connection. The
	 * {@link #beanName} together with a counter is used by default.
	 * @param connectionNameStrategy the {@link ConnectionNameStrategy} to use.
	 * @since 2.0
	 */
	public void setConnectionNameStrategy(ConnectionNameStrategy connectionNameStrategy) {
		this.connectionNameStrategy = connectionNameStrategy;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setConnectionNameStrategy(
					cf -> connectionNameStrategy.obtainNewConnectionName(cf) + PUBLISHER_SUFFIX);
		}
	}

	/**
	 * Set the strategy for logging close exceptions; by default, if a channel is closed due to a failed passive queue
	 * declaration, it is logged at debug level. Normal channel closes (200 OK) are not logged. All others are logged at
	 * ERROR level (unless access is refused due to an exclusive consumer condition, in which case, it is logged at
	 * DEBUG level, since 3.1, previously INFO).
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

	protected ConnectionNameStrategy getConnectionNameStrategy() {
		return this.connectionNameStrategy;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setBeanName(name + PUBLISHER_SUFFIX);
		}
	}

	/**
	 * Return a bean name of the component or null if not a bean.
	 * @return the bean name or null.
	 * @since 1.7.9
	 */
	protected @Nullable String getBeanName() {
		return this.beanName;
	}

	/**
	 * Set the mode for shuffling addresses.
	 * @param addressShuffleMode the address shuffle mode.
	 * @since 2.3
	 * @see Collections#shuffle(List)
	 */
	public void setAddressShuffleMode(AddressShuffleMode addressShuffleMode) {
		Assert.notNull(addressShuffleMode, "'addressShuffleMode' cannot be null");
		this.addressShuffleMode = addressShuffleMode; // NOSONAR - sync inconsistency
	}

	public boolean hasPublisherConnectionFactory() {
		return this.publisherConnectionFactory != null;
	}

	/**
	 * Set the backoff strategy for creating connections. This enhancement supports custom
	 * retry policies within the connection module, particularly useful when the maximum
	 * channel limit is reached. The {@link SimpleConnection#createChannel(boolean)} method
	 * utilizes this backoff strategy to gracefully handle such limit exceptions.
	 * @param backOff the backoff strategy to be applied during connection creation
	 * @since 3.1.3
	 */
	public void setConnectionCreatingBackOff(@Nullable BackOff backOff) {
		this.connectionCreatingBackOff = backOff;
	}

	@Override
	public @Nullable ConnectionFactory getPublisherConnectionFactory() {
		return this.publisherConnectionFactory;
	}

	protected final Connection createBareConnection() {
		try {
			String connectionName = this.connectionNameStrategy.obtainNewConnectionName(this);
			com.rabbitmq.client.Connection rabbitConnection = connect(connectionName);
			rabbitConnection.addShutdownListener(this);
			Connection connection = new SimpleConnection(rabbitConnection, this.closeTimeout,
					this.connectionCreatingBackOff == null ? null : this.connectionCreatingBackOff.start());
			if (rabbitConnection instanceof AutorecoveringConnection auto) {
				auto.addRecoveryListener(new RecoveryListener() {

					@Override
					public void handleRecoveryStarted(Recoverable recoverable) {
						handleRecovery(recoverable);
					}

					@Override
					public void handleRecovery(Recoverable recoverable) {
						try {
							connection.close();
						}
						catch (Exception e) {
							AbstractConnectionFactory.this.logger.error("Failed to close auto-recover connection", e);
						}
					}

				});
			}
			if (this.logger.isInfoEnabled()) {
				this.logger.info("Created new connection: " + connectionName + "/" + connection);
			}
			if (rabbitConnection instanceof AutorecoveringConnection auto) {
				auto.addRecoveryListener(this.recoveryListener);
			}

			if (this.applicationEventPublisher != null) {
				connection
						.addBlockedListener(new ConnectionBlockedListener(connection, this.applicationEventPublisher));
			}

			return connection;
		}
		catch (IOException | TimeoutException ex) {
			RuntimeException converted = RabbitExceptionTranslator.convertRabbitAccessException(ex);
			this.connectionListener.onFailed(ex);
			throw converted;
		}
	}

	private com.rabbitmq.client.Connection connect(String connectionName)
			throws IOException, TimeoutException {

		this.lock.lock();
		try {
			if (this.addressResolver != null) {
				return connectResolver(connectionName);
			}
			if (this.addresses != null) {
				return connectAddresses(connectionName);
			}
			else {
				return connectHostPort(connectionName);
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	private com.rabbitmq.client.Connection connectResolver(String connectionName) throws IOException, TimeoutException {
		if (this.logger.isInfoEnabled()) {
			this.logger.info("Attempting to connect with: " + this.addressResolver);
		}
		return this.rabbitConnectionFactory.newConnection(this.executorService, this.addressResolver,
				connectionName);
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	private com.rabbitmq.client.Connection connectAddresses(String connectionName)
			throws IOException, TimeoutException {

		this.lock.lock();
		try {
			List<Address> addressesToConnect = new ArrayList<>(this.addresses);
			if (addressesToConnect.size() > 1 && AddressShuffleMode.RANDOM.equals(this.addressShuffleMode)) {
				Collections.shuffle(addressesToConnect);
			}
			if (this.logger.isInfoEnabled()) {
				this.logger.info("Attempting to connect to: " + addressesToConnect);
			}
			com.rabbitmq.client.Connection connection = this.rabbitConnectionFactory.newConnection(this.executorService,
					addressesToConnect, connectionName);
			if (addressesToConnect.size() > 1 && AddressShuffleMode.INORDER.equals(this.addressShuffleMode)) {
				this.addresses.add(this.addresses.remove(0));
			}
			return connection;
		}
		finally {
			this.lock.unlock();
		}
	}

	private com.rabbitmq.client.Connection connectHostPort(String connectionName) throws IOException, TimeoutException {
		if (this.logger.isInfoEnabled()) {
			this.logger.info("Attempting to connect to: " + this.rabbitConnectionFactory.getHost()
					+ ":" + this.rabbitConnectionFactory.getPort());
		}
		return this.rabbitConnectionFactory.newConnection(this.executorService, connectionName);
	}

	protected final String getDefaultHostName() {
		String temp;
		try {
			InetAddress localMachine = InetAddress.getLocalHost();
			temp = localMachine.getHostName();
			this.logger.debug("Using hostname [" + temp + "] for hostname.");
		}
		catch (UnknownHostException e) {
			this.logger.warn("Could not get host name, using 'localhost' as default value", e);
			temp = "localhost";
		}
		return temp;
	}

	@Override
	public void shutdownCompleted(ShutdownSignalException cause) {
		Method reason = cause.getReason();
		int protocolClassId = RabbitUtils.CONNECTION_PROTOCOL_CLASS_ID_10;
		if (reason != null) {
			protocolClassId = reason.protocolClassId();
		}
		if (protocolClassId == RabbitUtils.CHANNEL_PROTOCOL_CLASS_ID_20) {
			this.closeExceptionLogger.log(this.logger, "Shutdown Signal", cause);
			getChannelListener().onShutDown(cause);
		}
		else if (protocolClassId == RabbitUtils.CONNECTION_PROTOCOL_CLASS_ID_10) {
			getConnectionListener().onShutDown(cause);
		}
	}

	@Override
	public void destroy() {
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.destroy();
		}
	}

	@Override
	public String toString() {
		if (this.beanName != null) {
			return this.beanName;
		}
		else {
			return super.toString();
		}
	}

	private record ConnectionBlockedListener(Connection connection, ApplicationEventPublisher applicationEventPublisher)
			implements BlockedListener {

		@Override
		public void handleBlocked(String reason) {
			this.applicationEventPublisher.publishEvent(new ConnectionBlockedEvent(this.connection, reason));
		}

		@Override
		public void handleUnblocked() {
			this.applicationEventPublisher.publishEvent(new ConnectionUnblockedEvent(this.connection));
		}

	}

	/**
	 * Default implementation of {@link ConditionalExceptionLogger} for logging channel close exceptions.
	 * @since 1.5
	 */
	public static class DefaultChannelCloseLogger implements ConditionalExceptionLogger {

		@Override
		public void log(Log logger, String message, @Nullable Throwable t) {
			if (t instanceof ShutdownSignalException cause) {
				if (RabbitUtils.isPassiveDeclarationChannelClose(cause)) {
					if (logger.isDebugEnabled()) {
						logger.debug(message + ": " + cause.getMessage());
					}
				}
				else if (RabbitUtils.isExclusiveUseChannelClose(cause)) {
					if (logger.isDebugEnabled()) {
						logger.debug(message + ": " + cause.getMessage());
					}
				}
				else if (!RabbitUtils.isNormalChannelClose(cause)) {
					logger.error(message + ": " + cause.getMessage());
				}
			}
			else {
				logger.error("Unexpected invocation of " + getClass() + ", with message: " + message, t);
			}
		}

	}

}
