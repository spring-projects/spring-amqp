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
import java.util.function.Consumer;

import com.rabbitmq.client.Channel;

import org.springframework.amqp.AmqpIOException;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Helper class for managing a Spring based Rabbit {@link org.springframework.amqp.rabbit.connection.ConnectionFactory},
 * in particular for obtaining transactional Rabbit resources for a given ConnectionFactory.
 *
 * <p>
 * Mainly for internal use within the framework. Used by {@link org.springframework.amqp.rabbit.core.RabbitTemplate} as
 * well as {@link org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer}.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
public final class ConnectionFactoryUtils {

	private static final boolean WEB_FLUX_PRESENT =
			ClassUtils.isPresent("org.springframework.web.reactive.function.client.WebClient",
				ConnectionFactoryUtils.class.getClassLoader());

	private static final ThreadLocal<AfterCompletionFailedException> COMPLETION_EXCEPTIONS = new ThreadLocal<>();

	private static boolean captureAfterCompletionExceptions;

	private ConnectionFactoryUtils() {
	}

	/**
	 * Determine whether the given RabbitMQ Channel is transactional, that is, bound to the current thread by Spring's
	 * transaction facilities.
	 * @param channel the RabbitMQ Channel to check
	 * @param connectionFactory the RabbitMQ ConnectionFactory that the Channel originated from
	 * @return whether the Channel is transactional
	 */
	public static boolean isChannelTransactional(Channel channel, ConnectionFactory connectionFactory) {
		if (channel == null || connectionFactory == null) {
			return false;
		}
		RabbitResourceHolder resourceHolder = (RabbitResourceHolder) TransactionSynchronizationManager
				.getResource(connectionFactory);
		return (resourceHolder != null && resourceHolder.containsChannel(channel));
	}

	/**
	 * Obtain a RabbitMQ Channel that is synchronized with the current transaction, if any.
	 * @param connectionFactory the ConnectionFactory to obtain a Channel for
	 * @param synchedLocalTransactionAllowed whether to allow for a local RabbitMQ transaction that is synchronized with
	 * a Spring-managed transaction (where the main transaction might be a JDBC-based one for a specific DataSource, for
	 * example), with the RabbitMQ transaction committing right after the main transaction. If not allowed, the given
	 * ConnectionFactory needs to handle transaction enlistment underneath the covers.
	 * @return the transactional Channel, or <code>null</code> if none found
	 */
	public static RabbitResourceHolder getTransactionalResourceHolder(final ConnectionFactory connectionFactory,
			final boolean synchedLocalTransactionAllowed) {
		return getTransactionalResourceHolder(connectionFactory, synchedLocalTransactionAllowed, false);
	}

	/**
	 * Obtain a RabbitMQ Channel that is synchronized with the current transaction, if any.
	 * @param connectionFactory the ConnectionFactory to obtain a Channel for
	 * @param synchedLocalTransactionAllowed whether to allow for a local RabbitMQ transaction that is synchronized with
	 * a Spring-managed transaction (where the main transaction might be a JDBC-based one for a specific DataSource, for
	 * example), with the RabbitMQ transaction committing right after the main transaction. If not allowed, the given
	 * ConnectionFactory needs to handle transaction enlistment underneath the covers.
	 * @param publisherConnectionIfPossible obtain a connection from a separate publisher connection
	 * if possible.
	 * @return the transactional Channel, or <code>null</code> if none found
	 */
	public static RabbitResourceHolder getTransactionalResourceHolder(final ConnectionFactory connectionFactory,
			final boolean synchedLocalTransactionAllowed, final boolean publisherConnectionIfPossible) {

		return doGetTransactionalResourceHolder(connectionFactory, new RabbitResourceFactory(connectionFactory,
				synchedLocalTransactionAllowed, publisherConnectionIfPossible));
	}

	/**
	 * Obtain a RabbitMQ Channel that is synchronized with the current transaction, if any.
	 * @param connectionFactory the RabbitMQ ConnectionFactory to bind for (used as TransactionSynchronizationManager
	 * key)
	 * @param resourceFactory the ResourceFactory to use for extracting or creating RabbitMQ resources
	 * @return the transactional Channel, or <code>null</code> if none found
	 */
	private static RabbitResourceHolder doGetTransactionalResourceHolder(// NOSONAR complexity
			ConnectionFactory connectionFactory, ResourceFactory resourceFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");
		Assert.notNull(resourceFactory, "ResourceFactory must not be null");

		RabbitResourceHolder resourceHolder = (RabbitResourceHolder) TransactionSynchronizationManager
				.getResource(connectionFactory);
		if (resourceHolder != null) {
			Channel channel = resourceFactory.getChannel(resourceHolder);
			if (channel != null) {
				return resourceHolder;
			}
		}
		RabbitResourceHolder resourceHolderToUse = resourceHolder;
		if (resourceHolderToUse == null) {
			resourceHolderToUse = new RabbitResourceHolder();
		}
		Connection connection = resourceFactory.getConnection(resourceHolderToUse); //NOSONAR
		Channel channel = null;
		try {
			/*
			 * If we are in a listener container, first see if there's a channel registered
			 * for this consumer and the consumer is using the same connection factory.
			 */
			channel = ConsumerChannelRegistry.getConsumerChannel(connectionFactory);
			if (channel == null && connection == null) {
				connection = resourceFactory.createConnection();
				if (resourceHolder == null) {
					/*
					 * While creating a connection, a connection listener might have created a
					 * transactional channel and bound it to the transaction.
					 */
					resourceHolder = (RabbitResourceHolder) TransactionSynchronizationManager
							.getResource(connectionFactory);
					if (resourceHolder != null) {
						channel = resourceHolder.getChannel();
						resourceHolderToUse = resourceHolder;
					}
				}
				resourceHolderToUse.addConnection(connection);
			}
			if (channel == null) {
				channel = resourceFactory.createChannel(connection);
			}
			resourceHolderToUse.addChannel(channel, connection);

			if (!resourceHolderToUse.equals(resourceHolder)
					&& TransactionSynchronizationManager.isSynchronizationActive()) {
				bindResourceToTransaction(resourceHolderToUse, connectionFactory,
						resourceFactory.isSynchedLocalTransactionAllowed());
			}

			return resourceHolderToUse;

		}
		catch (IOException ex) {
			RabbitUtils.closeConnection(connection);
			throw new AmqpIOException(ex);
		}
	}

	public static void releaseResources(@Nullable RabbitResourceHolder resourceHolder) {
		if (resourceHolder == null || resourceHolder.isSynchronizedWithTransaction()) {
			return;
		}
		RabbitUtils.closeChannel(resourceHolder.getChannel());
		RabbitUtils.closeConnection(resourceHolder.getConnection());
	}

	public static RabbitResourceHolder bindResourceToTransaction(RabbitResourceHolder resourceHolder,
			ConnectionFactory connectionFactory, boolean synched) {

		if (TransactionSynchronizationManager.hasResource(connectionFactory)
				|| !TransactionSynchronizationManager.isActualTransactionActive() || !synched) {
			return (RabbitResourceHolder) TransactionSynchronizationManager.getResource(connectionFactory); // NOSONAR never null
		}
		TransactionSynchronizationManager.bindResource(connectionFactory, resourceHolder);
		resourceHolder.setSynchronizedWithTransaction(true);
		if (TransactionSynchronizationManager.isSynchronizationActive()) {
			TransactionSynchronizationManager.registerSynchronization(new RabbitResourceSynchronization(resourceHolder,
					connectionFactory, ConnectionFactoryUtils::completionFailed));
		}
		return resourceHolder;
	}

	private static void completionFailed(AfterCompletionFailedException ex) {
		if (captureAfterCompletionExceptions) {
			COMPLETION_EXCEPTIONS.set(ex);
		}
	}

	/**
	 * Call this method to enable capturing {@link AfterCompletionFailedException}s
	 * when using transaction synchronization. Exceptions are stored in a {@link ThreadLocal}
	 * which must be cleared by calling {@link #checkAfterCompletion()} after the transaction
	 * has completed.
	 * @param enable true to enable capture.
	 */
	public static void enableAfterCompletionFailureCapture(boolean enable) {
		captureAfterCompletionExceptions = enable;
	}

	/**
	 * When using transaction synchronization, call this method after the transaction commits to
	 * verify that the RabbitMQ transaction committed.
	 * @throws AfterCompletionFailedException if synchronization failed.
	 * @since 2.3.10
	 */
	public static void checkAfterCompletion() {
		AfterCompletionFailedException ex = COMPLETION_EXCEPTIONS.get();
		if (ex != null) {
			COMPLETION_EXCEPTIONS.remove();
			throw ex;
		}
	}

	public static void registerDeliveryTag(ConnectionFactory connectionFactory, Channel channel, Long tag) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");

		RabbitResourceHolder resourceHolder = (RabbitResourceHolder) TransactionSynchronizationManager
				.getResource(connectionFactory);
		if (resourceHolder != null) {
			resourceHolder.addDeliveryTag(channel, tag);
		}
	}

	/**
	 * Create a connection with this connection factory and/or its publisher factory.
	 * @param connectionFactory the connection factory.
	 * @param publisherConnectionIfPossible true to use the publisher factory, if present.
	 * @return the connection.
	 * @since 2.0.2
	 */
	public static Connection createConnection(final ConnectionFactory connectionFactory,
			final boolean publisherConnectionIfPossible) {

		if (publisherConnectionIfPossible) {
			ConnectionFactory publisherFactory = connectionFactory.getPublisherConnectionFactory();
			if (publisherFactory != null) {
				return publisherFactory.createConnection();
			}
		}
		return connectionFactory.createConnection();
	}

	static NodeLocator<?> nodeLocator() {
		if (WEB_FLUX_PRESENT) {
			return new WebFluxNodeLocator();
		}
		else {
			return new RestTemplateNodeLocator();
		}
	}

	/**
	 * Callback interface for resource creation. Serving as argument for the <code>doGetTransactionalChannel</code>
	 * method.
	 */
	public interface ResourceFactory {

		/**
		 * Fetch an appropriate Channel from the given RabbitResourceHolder.
		 * @param holder the RabbitResourceHolder
		 * @return an appropriate Channel fetched from the holder, or <code>null</code> if none found
		 */
		@Nullable
		Channel getChannel(RabbitResourceHolder holder);

		/**
		 * Fetch an appropriate Connection from the given RabbitResourceHolder.
		 * @param holder the RabbitResourceHolder
		 * @return an appropriate Connection fetched from the holder, or <code>null</code> if none found
		 */
		Connection getConnection(RabbitResourceHolder holder);

		/**
		 * Create a new RabbitMQ Connection for registration with a RabbitResourceHolder.
		 * @return the new RabbitMQ Connection
		 * @throws IOException if thrown by RabbitMQ API methods
		 */
		Connection createConnection() throws IOException;

		/**
		 * Create a new RabbitMQ Session for registration with a RabbitResourceHolder.
		 * @param con the RabbitMQ Connection to create a Channel for
		 * @return the new RabbitMQ Channel
		 * @throws IOException if thrown by RabbitMQ API methods
		 */
		Channel createChannel(Connection con) throws IOException;

		/**
		 * Return whether to allow for a local RabbitMQ transaction that is synchronized with a Spring-managed
		 * transaction (where the main transaction might be a JDBC-based one for a specific DataSource, for example),
		 * with the RabbitMQ transaction committing right after the main transaction.
		 * @return whether to allow for synchronizing a local RabbitMQ transaction
		 */
		boolean isSynchedLocalTransactionAllowed();

	}

	private static class RabbitResourceFactory implements ResourceFactory {

		private final ConnectionFactory connectionFactory;

		private final boolean synchedLocalTransactionAllowed;

		private final boolean publisherConnectionIfPossible;

		RabbitResourceFactory(ConnectionFactory connectionFactory, boolean synchedLocalTransactionAllowed,
				boolean publisherConnectionIfPossible) {

			this.connectionFactory = connectionFactory;
			this.synchedLocalTransactionAllowed = synchedLocalTransactionAllowed;
			this.publisherConnectionIfPossible = publisherConnectionIfPossible;
		}

		@Override
		@Nullable
		public Channel getChannel(RabbitResourceHolder holder) {
			return holder.getChannel();
		}

		@Override
		@Nullable
		public Connection getConnection(RabbitResourceHolder holder) {
			return holder.getConnection();
		}

		@Override
		public Connection createConnection() {
			return ConnectionFactoryUtils.createConnection(this.connectionFactory,
					this.publisherConnectionIfPossible);
		}

		@Override
		public Channel createChannel(Connection con) {
			return con.createChannel(this.synchedLocalTransactionAllowed);
		}

		@Override
		public boolean isSynchedLocalTransactionAllowed() {
			return this.synchedLocalTransactionAllowed;
		}

	}

	/**
	 * Callback for resource cleanup at the end of a non-native RabbitMQ transaction (e.g. when participating in a
	 * JtaTransactionManager transaction).
	 * @see org.springframework.transaction.jta.JtaTransactionManager
	 */
	private static final class RabbitResourceSynchronization extends
			ResourceHolderSynchronization<RabbitResourceHolder, Object> {

		private final RabbitResourceHolder resourceHolder;

		private final Consumer<AfterCompletionFailedException> afterCompletionCallback;

		RabbitResourceSynchronization(RabbitResourceHolder resourceHolder, Object resourceKey,
				Consumer<AfterCompletionFailedException> afterCompletionCallback) {

			super(resourceHolder, resourceKey);
			this.resourceHolder = resourceHolder;
			this.afterCompletionCallback = afterCompletionCallback;
		}

		@Override
		protected boolean shouldReleaseBeforeCompletion() {
			return false;
		}

		@Override
		public void afterCompletion(int status) {
			try {
				if (status == TransactionSynchronization.STATUS_COMMITTED) {
					this.resourceHolder.commitAll();
				}
				else {
					this.resourceHolder.rollbackAll();
				}
			}
			catch (RuntimeException ex) {
				this.afterCompletionCallback.accept(new AfterCompletionFailedException(status, ex));
			}
			finally {
				if (this.resourceHolder.isReleaseAfterCompletion()) {
					this.resourceHolder.setSynchronizedWithTransaction(false);
				}
				super.afterCompletion(status);
			}
		}

		@Override
		protected void releaseResource(RabbitResourceHolder resourceHolder, Object resourceKey) {
			ConnectionFactoryUtils.releaseResources(resourceHolder);
		}

	}

}
