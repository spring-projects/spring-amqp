/*
 * Copyright 2020-present the original author or authors.
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.NameMatchMethodPointcutAdvisor;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

/**
 * A very simple connection factory that caches a channel per thread. Users are
 * responsible for releasing the thread's channel by calling
 * {@link #closeThreadChannel()}.
 *
 * @author Gary Russell
 * @author Leonardo Ferreira
 * @author Christian Tzolov
 * @author Ngoc Nhan
 * @author Artem Bilan
 *
 * @since 2.3
 *
 */
public class ThreadChannelConnectionFactory extends AbstractConnectionFactory
		implements ShutdownListener, SmartLifecycle {

	private final Lock lock = new ReentrantLock();

	private final Map<UUID, Context> contextSwitches = new ConcurrentHashMap<>();

	private final Map<UUID, Thread> switchesInProgress = new ConcurrentHashMap<>();

	private final AtomicBoolean running = new AtomicBoolean();

	private volatile @Nullable ConnectionWrapper connection;

	private boolean simplePublisherConfirms;

	private boolean defaultPublisherFactory = true;

	/**
	 * Construct an instance.
	 * @param rabbitConnectionFactory the rabbitmq connection factory.
	 */
	public ThreadChannelConnectionFactory(ConnectionFactory rabbitConnectionFactory) {
		this(rabbitConnectionFactory, false);
	}

	/**
	 * Construct an instance.
	 * @param rabbitConnectionFactory the rabbitmq connection factory.
	 * @param isPublisher true if we are creating a publisher connection factory.
	 */
	@SuppressWarnings("this-escape")
	private ThreadChannelConnectionFactory(ConnectionFactory rabbitConnectionFactory, boolean isPublisher) {
		super(rabbitConnectionFactory);
		if (!isPublisher) {
			doSetPublisherConnectionFactory(new ThreadChannelConnectionFactory(rabbitConnectionFactory, true));
		}
		else {
			this.defaultPublisherFactory = false;
		}
	}

	@Override
	public void setPublisherConnectionFactory(@Nullable AbstractConnectionFactory publisherConnectionFactory) {
		super.setPublisherConnectionFactory(publisherConnectionFactory);
		this.defaultPublisherFactory = false;
	}

	@Override
	public boolean isSimplePublisherConfirms() {
		return this.simplePublisherConfirms;
	}

	/**
	 * Enable simple publisher confirms.
	 * @param simplePublisherConfirms true to enable.
	 */
	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	public void setSimplePublisherConfirms(boolean simplePublisherConfirms) {
		this.simplePublisherConfirms = simplePublisherConfirms;
		if (this.defaultPublisherFactory) {
			((ThreadChannelConnectionFactory) getPublisherConnectionFactory())
					.setSimplePublisherConfirms(simplePublisherConfirms); // NOSONAR
		}
	}

	@Override
	public int getPhase() {
		return Integer.MIN_VALUE;
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

	@Override
	public void addConnectionListener(ConnectionListener listener) {
		super.addConnectionListener(listener); // handles publishing sub-factory
		// If the connection is already alive we assume that the new listener wants to be notified
		ConnectionWrapper connectionWrapper = this.connection;
		if (connectionWrapper != null && connectionWrapper.isOpen()) {
			listener.onCreate(connectionWrapper);
		}
	}

	@Override
	public Connection createConnection() throws AmqpException {
		ConnectionWrapper connectionWrapper = this.connection;
		if (connectionWrapper == null || !connectionWrapper.isOpen()) {
			this.lock.lock();
			try {
				connectionWrapper = this.connection;
				if (connectionWrapper == null || !connectionWrapper.isOpen()) {
					Connection bareConnection = createBareConnection();
					connectionWrapper = new ConnectionWrapper(bareConnection.getDelegate(), getCloseTimeout());
					this.connection = connectionWrapper;
					getConnectionListener().onCreate(this.connection);
				}
			}
			finally {
				this.lock.unlock();
			}
		}
		return connectionWrapper;
	}

	/**
	 * Close the channel associated with this thread, if any.
	 */
	public void closeThreadChannel() {
		ConnectionWrapper connectionWrapper = this.connection;
		if (connectionWrapper != null) {
			connectionWrapper.closeThreadChannel();
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
		destroy();
	}

	@Override
	public void destroy() {
		this.lock.lock();
		try {
			super.destroy();
			ConnectionWrapper connectionWrapper = this.connection;
			if (connectionWrapper != null) {
				connectionWrapper.forceClose();
				this.connection = null;
			}
			if (!this.switchesInProgress.isEmpty() && this.logger.isWarnEnabled()) {
				this.logger.warn("Unclaimed context switches from threads:" +
						this.switchesInProgress.values()
								.stream()
								.map(Thread::getName)
								.toList());
			}
			this.contextSwitches.clear();
			this.switchesInProgress.clear();
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Call to prepare to switch the channel(s) owned by this thread to another thread.
	 * @return an opaque object representing the context to switch. If there are no channels
	 * or no open channels assigned to this thread, null is returned.
	 * @since 2.3.7
	 * @see #switchContext(Object)
	 */
	public @Nullable Object prepareSwitchContext() {
		return prepareSwitchContext(UUID.randomUUID());
	}

	@SuppressWarnings("resource")
	@Nullable
	Object prepareSwitchContext(UUID uuid) {
		Object pubContext = null;
		if (getPublisherConnectionFactory() instanceof ThreadChannelConnectionFactory tccf) {
			pubContext = tccf.prepareSwitchContext(uuid);
		}
		Context context = ((ConnectionWrapper) createConnection()).prepareSwitchContext();
		if (context.nonTx() == null && context.tx() == null) {
			this.logger.debug("No channels are bound to this thread");
			return pubContext;
		}
		if (this.switchesInProgress.containsValue(Thread.currentThread())) {
			this.logger
					.warn("A previous context switch from this thread has not been claimed yet; possible memory leak?");
		}
		this.contextSwitches.put(uuid, context);
		this.switchesInProgress.put(uuid, Thread.currentThread());
		return uuid;
	}

	/**
	 * Acquire ownership of another thread's channel(s) after that thread called
	 * {@link #prepareSwitchContext()}.
	 * @param toSwitch the context returned by {@link #prepareSwitchContext()}.
	 * @since 2.3.7
	 * @see #prepareSwitchContext()
	 */
	public void switchContext(@Nullable Object toSwitch) {
		if (toSwitch != null) {
			Assert.state(doSwitch(toSwitch), () -> "No context to switch for " + toSwitch.toString());
		}
		else {
			this.logger.debug("Attempted to switch a null context - no channels to acquire");
		}
	}

	@SuppressWarnings("resource")
	boolean doSwitch(Object toSwitch) {
		boolean switched = false;
		if (getPublisherConnectionFactory() instanceof ThreadChannelConnectionFactory tccf) {
			switched = tccf.doSwitch(toSwitch); // NOSONAR
		}
		Context context = this.contextSwitches.remove(toSwitch);
		this.switchesInProgress.remove(toSwitch);
		if (context != null) {
			((ConnectionWrapper) createConnection()).switchContext(context);
			switched = true;
		}
		return switched;
	}

	private final class ConnectionWrapper extends SimpleConnection {

		/*
		 * Intentionally not static.
		 */
		private final Map<Thread, Channel> channels = new ConcurrentHashMap<>();

		private final Map<Thread, Channel> txChannels = new ConcurrentHashMap<>();

		ConnectionWrapper(com.rabbitmq.client.Connection delegate, int closeTimeout) {
			super(delegate, closeTimeout);
		}

		@SuppressWarnings("resource")
		@Override
		public Channel createChannel(boolean transactional) {
			Thread currentThread = Thread.currentThread();
			Map<Thread, Channel> channelMap = transactional ? this.txChannels : this.channels;
			Channel channel = channelMap.get(currentThread);
			if (channel == null || !channel.isOpen()) {
				channel = createProxy(super.createChannel(transactional), transactional);
				if (transactional) {
					try {
						channel.txSelect();
					}
					catch (IOException e) {
						throw RabbitExceptionTranslator.convertRabbitAccessException(e);
					}
					this.txChannels.put(currentThread, channel);
				}
				else {
					if (ThreadChannelConnectionFactory.this.simplePublisherConfirms) {
						try {
							channel.confirmSelect();
						}
						catch (IOException e) {
							throw RabbitExceptionTranslator.convertRabbitAccessException(e);
						}
					}
					this.channels.put(currentThread, channel);
				}
				getChannelListener().onCreate(channel, transactional);
			}
			return channel;
		}

		private Channel createProxy(Channel channel, boolean transactional) {
			ProxyFactory pf = new ProxyFactory(channel);
			AtomicBoolean confirmSelected = new AtomicBoolean();
			Advice advice =
					(MethodInterceptor) invocation -> {
						String method = invocation.getMethod().getName();
						return switch (method) {
							case "close" -> {
								handleClose(channel, transactional);
								yield null;
							}
							case "getTargetChannel" -> channel;
							case "isTransactional" -> transactional;
							case "confirmSelect" -> {
								confirmSelected.set(true);
								yield channel.confirmSelect();
							}
							case "isConfirmSelected" -> confirmSelected.get();
							case "isPublisherConfirms" -> false;
							default -> null;
						};
					};
			NameMatchMethodPointcutAdvisor advisor = new NameMatchMethodPointcutAdvisor(advice);
			advisor.addMethodName("close");
			advisor.addMethodName("getTargetChannel");
			advisor.addMethodName("isTransactional");
			advisor.addMethodName("confirmSelect");
			advisor.addMethodName("isConfirmSelected");
			advisor.addMethodName("isPublisherConfirms");
			pf.addAdvisor(advisor);
			pf.addInterface(ChannelProxy.class);
			return (Channel) pf.getProxy();
		}

		private void handleClose(Channel channel, boolean transactional) {
			Thread currentThread = Thread.currentThread();
			Map<Thread, Channel> channelMap = transactional ? this.txChannels : this.channels;
			if (channelMap.get(currentThread) == null) {
				physicalClose(channel);
			}
			else {
				if (RabbitUtils.isPhysicalCloseRequired()) {
					physicalClose(channel);
					channelMap.remove(currentThread);
				}
			}
		}

		@Override
		public void close() {
		}

		@Override
		public void closeThreadChannel() {
			doClose(this.channels);
			doClose(this.txChannels);
		}

		private void doClose(Map<Thread, Channel> channelMap) {
			Channel channel = channelMap.remove(Thread.currentThread());
			if (channel != null) {
				physicalClose(channel);
			}
		}

		private void physicalClose(Channel channel) {
			if (channel.isOpen()) {
				try {
					channel.close();
				}
				catch (IOException | TimeoutException e) {
					logger.debug("Error on close", e);
				}
				finally {
					RabbitUtils.clearPhysicalCloseRequired();
				}
			}
		}

		void forceClose() {
			super.close();
			getConnectionListener().onClose(this);
		}

		Context prepareSwitchContext() {
			Thread currentThread = Thread.currentThread();
			Context context = new Context(this.channels.remove(currentThread), this.txChannels.remove(currentThread));
			return context;
		}

		void switchContext(Context context) {
			Channel nonTx = context.nonTx();
			if (nonTx != null) {
				doSwitch(nonTx, this.channels);
			}
			Channel tx = context.tx();
			if (tx != null) {
				doSwitch(tx, this.txChannels);
			}
		}

		private void doSwitch(Channel channel, Map<Thread, Channel> channelMap) {
			Thread currentThread = Thread.currentThread();
			Channel toClose = channelMap.get(currentThread);
			if (toClose != null) {
				RabbitUtils.setPhysicalCloseRequired(channel, true);
				physicalClose(toClose);
			}
			channelMap.put(currentThread, channel);
		}

	}

	private record Context(@Nullable Channel nonTx, @Nullable Channel tx) {

	}

}
