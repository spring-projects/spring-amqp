/*
 * Copyright 2020-2023 the original author or authors.
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.NameMatchMethodPointcutAdvisor;
import org.springframework.context.SmartLifecycle;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;

/**
 * A very simple connection factory that caches channels using Apache Pool2
 * {@link GenericObjectPool}s (one for transactional and one for non-transactional
 * channels). The pools have default configuration but they can be configured using
 * a callback.
 *
 * @author Gary Russell
 * @author Leonardo Ferreira
 * @author Christian Tzolov
 * @since 2.3
 *
 */
public class PooledChannelConnectionFactory extends AbstractConnectionFactory
		implements ShutdownListener, SmartLifecycle {

	private final AtomicBoolean running = new AtomicBoolean();

	private final Lock lock = new ReentrantLock();

	private volatile ConnectionWrapper connection;

	private boolean simplePublisherConfirms;

	private BiConsumer<GenericObjectPool<Channel>, Boolean> poolConfigurer = (pool, tx) -> { };

	private boolean defaultPublisherFactory = true;

	/**
	 * Construct an instance.
	 * @param rabbitConnectionFactory the rabbitmq connection factory.
	 */
	public PooledChannelConnectionFactory(ConnectionFactory rabbitConnectionFactory) {
		this(rabbitConnectionFactory, false);
	}

	/**
	 * Construct an instance.
	 * @param rabbitConnectionFactory the rabbitmq connection factory.
	 * @param isPublisher true if we are creating a publisher connection factory.
	 */
	private PooledChannelConnectionFactory(ConnectionFactory rabbitConnectionFactory, boolean isPublisher) {
		super(rabbitConnectionFactory);
		if (!isPublisher) {
			doSetPublisherConnectionFactory(new PooledChannelConnectionFactory(rabbitConnectionFactory, true));
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

	/**
	 * Add a consumer to configure the object pool. The second argument is true when
	 * called with the transactional pool.
	 * @param poolConfigurer the configurer.
	 */
	public void setPoolConfigurer(BiConsumer<GenericObjectPool<Channel>, Boolean> poolConfigurer) {
		Assert.notNull(poolConfigurer, "'poolConfigurer' cannot be null");
		this.poolConfigurer = poolConfigurer; // NOSONAR - sync inconsistency
		if (this.defaultPublisherFactory) {
			((PooledChannelConnectionFactory) getPublisherConnectionFactory()).setPoolConfigurer(poolConfigurer); // NOSONAR
		}
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
			((PooledChannelConnectionFactory) getPublisherConnectionFactory())
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
	public Connection createConnection() throws AmqpException {
		this.lock.lock();
		try {
			if (this.connection == null || !this.connection.isOpen()) {
				Connection bareConnection = createBareConnection(); // NOSONAR - see destroy()
				this.connection = new ConnectionWrapper(bareConnection.getDelegate(), getCloseTimeout(), // NOSONAR
						this.simplePublisherConfirms, this.poolConfigurer, getChannelListener()); // NOSONAR
				getConnectionListener().onCreate(this.connection);
			}
			return this.connection;
		}
		finally {
			this.lock.unlock();
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
			if (this.connection != null) {
				this.connection.forceClose();
				getConnectionListener().onClose(this.connection);
				this.connection = null;
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	private static final class ConnectionWrapper extends SimpleConnection {

		private static final Log LOGGER = LogFactory.getLog(ConnectionWrapper.class);

		private final ObjectPool<Channel> channels;

		private final ObjectPool<Channel> txChannels;

		private final boolean simplePublisherConfirms;

		private final ChannelListener channelListener;

		ConnectionWrapper(com.rabbitmq.client.Connection delegate, int closeTimeout, boolean simplePublisherConfirms,
				BiConsumer<GenericObjectPool<Channel>, Boolean> configurer, ChannelListener channelListener) {

			super(delegate, closeTimeout);
			this.channels = createPool(new ChannelFactory(), configurer, false);
			this.txChannels = createPool(new TxChannelFactory(), configurer, true);
			this.simplePublisherConfirms = simplePublisherConfirms;
			this.channelListener = channelListener;
		}

		private GenericObjectPool<Channel> createPool(ChannelFactory channelFactory,
				BiConsumer<GenericObjectPool<Channel>, Boolean> configurer, boolean tx) {

			GenericObjectPool<Channel> pool = new GenericObjectPool<>(channelFactory);
			configurer.accept(pool, tx);
			return pool;
		}

		@Override
		public Channel createChannel(boolean transactional) {
			try {
				Channel channel = transactional ? this.txChannels.borrowObject() : this.channels.borrowObject();
				this.channelListener.onCreate(channel, transactional);
				return channel;
			}
			catch (Exception e) {
				throw RabbitExceptionTranslator.convertRabbitAccessException(e);
			}
		}

		private Channel createProxy(Channel channel, boolean transacted) {
			ProxyFactory pf = new ProxyFactory(channel);
			AtomicReference<Channel> proxy = new AtomicReference<>();
			AtomicBoolean confirmSelected = new AtomicBoolean();
			Advice advice =
					(MethodInterceptor) invocation -> {
						String method = invocation.getMethod().getName();
						switch (method) {
						case "close":
							handleClose(channel, transacted, proxy);
							return null;
						case "getTargetChannel":
							return channel;
						case "isTransactional":
							return transacted;
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
			proxy.set((Channel) pf.getProxy());
			return proxy.get();
		}

		private void handleClose(Channel channel, boolean transacted, AtomicReference<Channel> proxy)
				throws Exception { // NOSONAR returnObject() throws it

			if (!RabbitUtils.isPhysicalCloseRequired()) {
				if (transacted) {
					ConnectionWrapper.this.txChannels.returnObject(proxy.get());
				}
				else {
					ConnectionWrapper.this.channels.returnObject(proxy.get());
				}
			}
			else {
				physicalClose(channel);
			}
		}

		@Override
		public void close() {
		}

		void forceClose() {
			super.close();
			this.channels.close();
			this.txChannels.close();
		}

		private void physicalClose(Channel channel) {
			RabbitUtils.clearPhysicalCloseRequired();
			if (channel.isOpen()) {
				try {
					channel.close();
				}
				catch (IOException | TimeoutException e) {
					LOGGER.debug("Error on close", e);
				}
			}
		}

		private class ChannelFactory implements PooledObjectFactory<Channel> {

			@Override
			public PooledObject<Channel> makeObject() {
				Channel channel = createProxy(ConnectionWrapper.super.createChannel(false), false);
				if (ConnectionWrapper.this.simplePublisherConfirms) {
					try {
						channel.confirmSelect();
					}
					catch (IOException e) {
						throw RabbitExceptionTranslator.convertRabbitAccessException(e);
					}
				}
				return new DefaultPooledObject<>(channel);
			}

			@Override
			public void destroyObject(PooledObject<Channel> p) throws Exception {
				Channel channel = p.getObject();
				if (channel instanceof ChannelProxy) {
					channel = ((ChannelProxy) channel).getTargetChannel();
				}

				ConnectionWrapper.this.physicalClose(channel);
			}

			@Override
			public boolean validateObject(PooledObject<Channel> p) {
				return p.getObject().isOpen();
			}

			@Override
			public void activateObject(PooledObject<Channel> p) {
			}

			@Override
			public void passivateObject(PooledObject<Channel> p) {
			}

		}

		private final class TxChannelFactory extends ChannelFactory {

			@Override
			public PooledObject<Channel> makeObject() {
				Channel channel = createProxy(ConnectionWrapper.super.createChannel(true), true);
				try {
					channel.txSelect();
				}
				catch (IOException e) {
					throw RabbitExceptionTranslator.convertRabbitAccessException(e);
				}
				return new DefaultPooledObject<>(channel);
			}

		}

	}

}
