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
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
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

import com.rabbitmq.client.Address;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Steve Powell
 * @author Artem Bilan
 *
 */
public abstract class AbstractConnectionFactory implements ConnectionFactory, DisposableBean, BeanNameAware,
		ApplicationContextAware, ApplicationEventPublisherAware, ApplicationListener<ContextClosedEvent> {

	private static final String PUBLISHER_SUFFIX = ".publisher";

	public static final int DEFAULT_CLOSE_TIMEOUT = 30000;

	private static final String BAD_URI = "setUri() was passed an invalid URI; it is ignored";

	protected final Log logger = LogFactory.getLog(getClass());

	private final com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory;

	private final CompositeConnectionListener connectionListener = new CompositeConnectionListener();

	private final CompositeChannelListener channelListener = new CompositeChannelListener();

	private final AtomicInteger defaultConnectionNameStrategyCounter = new AtomicInteger();

	private AbstractConnectionFactory publisherConnectionFactory;

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

	private ExecutorService executorService;

	private Address[] addresses;

	private int closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	private ConnectionNameStrategy connectionNameStrategy =
			connectionFactory -> (this.beanName != null ? this.beanName : "SpringAMQP") +
					"#" + ObjectUtils.getIdentityHexString(this) + ":" +
					this.defaultConnectionNameStrategyCounter.getAndIncrement();

	private String beanName;

	private ApplicationContext applicationContext;

	private ApplicationEventPublisher applicationEventPublisher;

	private volatile boolean contextStopped;

	/**
	 * Create a new AbstractConnectionFactory for the given target ConnectionFactory,
	 * with no publisher connection factory.
	 * @param rabbitConnectionFactory the target ConnectionFactory
	 */
	public AbstractConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory) {
		Assert.notNull(rabbitConnectionFactory, "Target ConnectionFactory must not be null");
		this.rabbitConnectionFactory = rabbitConnectionFactory;
	}

	protected final void setPublisherConnectionFactory(
			AbstractConnectionFactory publisherConnectionFactory) {
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

	protected ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}

	@Override
	public void onApplicationEvent(ContextClosedEvent event) {
		if (getApplicationContext() == event.getApplicationContext()) {
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
	 * Return the user name from the underlying rabbit connection factory.
	 * @return the user name.
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
	 * Set addresses for clustering.
	 * This property overrides the host+port properties if not empty.
	 * @param addresses list of addresses with form "host[:port],..."
	 */
	public void setAddresses(String addresses) {
		if (StringUtils.hasText(addresses)) {
			Address[] addressArray = Address.parseAddresses(addresses);
			if (addressArray.length > 0) {
				this.addresses = addressArray;
				if (this.publisherConnectionFactory != null) {
					this.publisherConnectionFactory.setAddresses(addresses);
				}
				return;
			}
		}
		this.logger.info("setAddresses() called with an empty value, will be using the host+port properties for connections");
		this.addresses = null;
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
	 * Provide an Executor for
	 * use by the Rabbit ConnectionFactory when creating connections.
	 * Can either be an ExecutorService or a Spring
	 * ThreadPoolTaskExecutor, as defined by a &lt;task:executor/&gt; element.
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

	protected ExecutorService getExecutorService() {
		return this.executorService;
	}

	/**
	 * How long to wait (milliseconds) for a response to a connection close
	 * operation from the broker; default 30000 (30 seconds).
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
	 * Provide a {@link ConnectionNameStrategy} to build the name for the target RabbitMQ connection.
	 * The {@link #beanName} together with a counter is used by default.
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

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
		if (this.publisherConnectionFactory != null) {
			this.publisherConnectionFactory.setBeanName(name + PUBLISHER_SUFFIX);
		}
	}

	public boolean hasPublisherConnectionFactory() {
		return this.publisherConnectionFactory != null;
	}

	@Override
	public ConnectionFactory getPublisherConnectionFactory() {
		return this.publisherConnectionFactory;
	}

	protected final Connection createBareConnection() {
		try {
			String connectionName = this.connectionNameStrategy.obtainNewConnectionName(this);

			com.rabbitmq.client.Connection rabbitConnection;
			if (this.addresses != null) {
				if (this.logger.isInfoEnabled()) {
					this.logger.info("Attempting to connect to: " + Arrays.toString(this.addresses));
				}
				rabbitConnection = this.rabbitConnectionFactory.newConnection(this.executorService, this.addresses,
						connectionName);

			}
			else {
				if (this.logger.isInfoEnabled()) {
					this.logger.info("Attempting to connect to: " + this.rabbitConnectionFactory.getHost()
							+ ":" + this.rabbitConnectionFactory.getPort());
				}
				rabbitConnection = this.rabbitConnectionFactory.newConnection(this.executorService, connectionName);
			}

			Connection connection = new SimpleConnection(rabbitConnection, this.closeTimeout);
			if (this.logger.isInfoEnabled()) {
				this.logger.info("Created new connection: " + connectionName + "/" + connection);
			}
			if (this.recoveryListener != null && rabbitConnection instanceof AutorecoveringConnection) {
				((AutorecoveringConnection) rabbitConnection).addRecoveryListener(this.recoveryListener);
			}

			if (this.applicationEventPublisher != null) {
				connection.addBlockedListener(new ConnectionBlockedListener(connection, this.applicationEventPublisher));
			}

			return connection;
		}
		catch (IOException | TimeoutException e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
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

	private static final class ConnectionBlockedListener implements BlockedListener {

		private final Connection connection;

		private final ApplicationEventPublisher applicationEventPublisher;

		ConnectionBlockedListener(Connection connection, ApplicationEventPublisher applicationEventPublisher) {
			this.connection = connection;
			this.applicationEventPublisher = applicationEventPublisher;
		}

		@Override
		public void handleBlocked(String reason) throws IOException {
			this.applicationEventPublisher.publishEvent(new ConnectionBlockedEvent(this.connection, reason));
		}

		@Override
		public void handleUnblocked() throws IOException {
			this.applicationEventPublisher.publishEvent(new ConnectionUnblockedEvent(this.connection));
		}

	}

}
