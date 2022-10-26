/*
 * Copyright 2020-2022 the original author or authors.
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
import java.util.stream.Collectors;

import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.NameMatchMethodPointcutAdvisor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;

/**
 * A very simple connection factory that caches a channel per thread. Users are
 * responsible for releasing the thread's channel by calling
 * {@link #closeThreadChannel()}.
 *
 * @author Gary Russell
 * @author Leonardo Ferreira
 * @since 2.3
 *
 */
public class ThreadChannelConnectionFactory extends AbstractConnectionFactory implements ShutdownListener {

	private final Map<UUID, Context> contextSwitches = new ConcurrentHashMap<>();

	private final Map<UUID, Thread> switchesInProgress = new ConcurrentHashMap<>();

	private volatile ConnectionWrapper connection;

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
	private ThreadChannelConnectionFactory(ConnectionFactory rabbitConnectionFactory, boolean isPublisher) {
		super(rabbitConnectionFactory);
		if (!isPublisher) {
			setPublisherConnectionFactory(new ThreadChannelConnectionFactory(rabbitConnectionFactory, true));
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
	public void setSimplePublisherConfirms(boolean simplePublisherConfirms) {
		this.simplePublisherConfirms = simplePublisherConfirms;
		if (this.defaultPublisherFactory) {
			((ThreadChannelConnectionFactory) getPublisherConnectionFactory())
				.setSimplePublisherConfirms(simplePublisherConfirms); // NOSONAR
		}
	}

	@Override
	public void addConnectionListener(ConnectionListener listener) {
		super.addConnectionListener(listener); // handles publishing sub-factory
		// If the connection is already alive we assume that the new listener wants to be notified
		if (this.connection != null && this.connection.isOpen()) {
			listener.onCreate(this.connection);
		}
	}

	@Override
	public synchronized Connection createConnection() throws AmqpException {
		if (this.connection == null || !this.connection.isOpen()) {
			Connection bareConnection = createBareConnection(); // NOSONAR - see destroy()
			this.connection = new ConnectionWrapper(bareConnection.getDelegate(), getCloseTimeout()); // NOSONAR
			getConnectionListener().onCreate(this.connection);
		}
		return this.connection;
	}

	/**
	 * Close the channel associated with this thread, if any.
	 */
	public void closeThreadChannel() {
		ConnectionWrapper connection2 = this.connection;
		if (connection2 != null) {
			connection2.closeThreadChannel();
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
	public synchronized void destroy() {
		super.destroy();
		if (this.connection != null) {
			this.connection.forceClose();
			this.connection = null;
		}
		if (this.switchesInProgress.size() > 0 && this.logger.isWarnEnabled()) {
			this.logger.warn("Unclaimed context switches from threads:" +
					this.switchesInProgress.values()
							.stream()
							.map(t -> t.getName())
							.collect(Collectors.toList()));
		}
		this.contextSwitches.clear();
		this.switchesInProgress.clear();
	}

	/**
	 * Call to prepare to switch the channel(s) owned by this thread to another thread.
	 * @return an opaque object representing the context to switch. If there are no channels
	 * or no open channels assigned to this thread, null is returned.
	 * @since 2.3.7
	 * @see #switchContext(Object)
	 */
	@Nullable
	public Object prepareSwitchContext() {
		return prepareSwitchContext(UUID.randomUUID());
	}

	@Nullable
	Object prepareSwitchContext(UUID uuid) {
		Object pubContext = null;
		if (getPublisherConnectionFactory() instanceof ThreadChannelConnectionFactory) {
			pubContext = ((ThreadChannelConnectionFactory) getPublisherConnectionFactory()).prepareSwitchContext(uuid); // NOSONAR
		}
		Context context = ((ConnectionWrapper) createConnection()).prepareSwitchContext();
		if (context.getNonTx() == null && context.getTx() == null) {
			this.logger.debug("No channels are bound to this thread");
			return pubContext;
		}
		if (this.switchesInProgress.values().contains(Thread.currentThread())) {
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

	boolean doSwitch(Object toSwitch) {
		boolean switched = false;
		if (getPublisherConnectionFactory() instanceof ThreadChannelConnectionFactory) {
			switched = ((ThreadChannelConnectionFactory) getPublisherConnectionFactory()).doSwitch(toSwitch); // NOSONAR
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
		private final ThreadLocal<Channel> channels = new ThreadLocal<>();

		private final ThreadLocal<Channel> txChannels = new ThreadLocal<>();

		ConnectionWrapper(com.rabbitmq.client.Connection delegate, int closeTimeout) {
			super(delegate, closeTimeout);
		}

		@SuppressWarnings("resource")
		@Override
		public Channel createChannel(boolean transactional) {
			Channel channel = transactional ? this.txChannels.get() : this.channels.get();
			if (channel == null || !channel.isOpen()) {
				channel = createProxy(super.createChannel(transactional), transactional);
				if (transactional) {
					try {
						channel.txSelect();
					}
					catch (IOException e) {
						throw RabbitExceptionTranslator.convertRabbitAccessException(e);
					}
					this.txChannels.set(channel);
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
					this.channels.set(channel);
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
						switch (method) {
							case "close":
								handleClose(channel, transactional);
								return null;
							case "getTargetChannel":
								return channel;
							case "isTransactional":
								return transactional;
							case "confirmSelect":
								confirmSelected.set(true);
								return channel.confirmSelect();
							case "isConfirmSelected":
								return confirmSelected.get();
							case "isPublisherConfirms":
								return false;
						}
						return null;
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
			if ((transactional && this.txChannels.get() == null) || (!transactional && this.channels.get() == null)) {
				physicalClose(channel);
			}
			else {
				if (RabbitUtils.isPhysicalCloseRequired()) {
					physicalClose(channel);
					if (transactional) {
						this.txChannels.remove();
					}
					else {
						this.channels.remove();
					}
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

		private void doClose(ThreadLocal<Channel> channelsTL) {
			Channel channel = channelsTL.get();
			if (channel != null) {
				channelsTL.remove();
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
			Context context = new Context(this.channels.get(), this.txChannels.get());
			this.channels.remove();
			this.txChannels.remove();
			return context;
		}

		void switchContext(Context context) {
			Channel nonTx = context.getNonTx();
			if (nonTx != null) {
				doSwitch(nonTx, this.channels);
			}
			Channel tx = context.getTx();
			if (tx != null) {
				doSwitch(tx, this.txChannels);
			}
		}

		private void doSwitch(Channel channel, ThreadLocal<Channel> channelTL) {
			Channel toClose = channelTL.get();
			if (toClose != null) {
				RabbitUtils.setPhysicalCloseRequired(channel, true);
				physicalClose(toClose);
			}
			channelTL.set(channel);
		}

	}

	private static class Context {

		private final Channel nonTx;

		private final Channel tx;

		Context(@Nullable Channel nonTx, @Nullable Channel tx) {
			this.nonTx = nonTx;
			this.tx = tx;
		}

		@Nullable
		Channel getNonTx() {
			return this.nonTx;
		}

		@Nullable
		Channel getTx() {
			return this.tx;
		}

	}

}
