/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.transaction;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.rabbitmq.client.Connection;

/**
 * {@link org.springframework.transaction.PlatformTransactionManager} implementation for a single Rabbit
 * {@link ConnectionFactory}. Binds a Rabbit Channel from the specified ConnectionFactory to the thread, potentially
 * allowing for one thread-bound channel per ConnectionFactory.
 *
 * <p>
 * This local strategy is an alternative to executing Rabbit operations within, and synchronized with, external
 * transactions. This strategy is <i>not</i> able to provide XA transactions, for example in order to share transactions
 * between messaging and database access.
 *
 * <p>
 * Application code is required to retrieve the transactional Rabbit resources via
 * {@link ConnectionFactoryUtils#getTransactionalResourceHolder(ConnectionFactory, boolean)} instead of a standard
 * {@link Connection#createChannel()} call with subsequent Channel creation. Spring's {@link RabbitTemplate} will
 * autodetect a thread-bound Channel and automatically participate in it.
 *
 * <p>
 * <b>The use of {@link CachingConnectionFactory} as a target for this transaction manager is strongly recommended.</b>
 * CachingConnectionFactory uses a single Rabbit Connection for all Rabbit access in order to avoid the overhead of
 * repeated Connection creation, as well as maintaining a cache of Channels. Each transaction will then share the same
 * Rabbit Connection, while still using its own individual Rabbit Channel.
 *
 * <p>
 * Transaction synchronization is turned off by default, as this manager might be used alongside a datastore-based
 * Spring transaction manager such as the JDBC org.springframework.jdbc.datasource.DataSourceTransactionManager,
 * which has stronger needs for synchronization.
 *
 * @author Dave Syer
 */
@SuppressWarnings("serial")
public class RabbitTransactionManager extends AbstractPlatformTransactionManager
		implements ResourceTransactionManager, InitializingBean {

	private ConnectionFactory connectionFactory;

	/**
	 * Create a new RabbitTransactionManager for bean-style usage.
	 * <p>
	 * Note: The ConnectionFactory has to be set before using the instance. This constructor can be used to prepare a
	 * RabbitTemplate via a BeanFactory, typically setting the ConnectionFactory via setConnectionFactory.
	 * <p>
	 * Turns off transaction synchronization by default, as this manager might be used alongside a datastore-based
	 * Spring transaction manager like DataSourceTransactionManager, which has stronger needs for synchronization. Only
	 * one manager is allowed to drive synchronization at any point of time.
	 * @see #setConnectionFactory
	 * @see #setTransactionSynchronization
	 */
	public RabbitTransactionManager() {
		setTransactionSynchronization(SYNCHRONIZATION_NEVER);
	}

	/**
	 * Create a new RabbitTransactionManager, given a ConnectionFactory.
	 * @param connectionFactory the ConnectionFactory to use
	 */
	public RabbitTransactionManager(ConnectionFactory connectionFactory) {
		this();
		this.connectionFactory = connectionFactory;
		afterPropertiesSet();
	}

	/**
	 * @param connectionFactory the connectionFactory to set
	 */
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @return the connectionFactory
	 */
	public ConnectionFactory getConnectionFactory() {
		return this.connectionFactory;
	}

	/**
	 * Make sure the ConnectionFactory has been set.
	 */
	public void afterPropertiesSet() {
		if (getConnectionFactory() == null) {
			throw new IllegalArgumentException("Property 'connectionFactory' is required");
		}
	}

	public Object getResourceFactory() {
		return getConnectionFactory();
	}

	@Override
	protected Object doGetTransaction() {
		RabbitTransactionObject txObject = new RabbitTransactionObject();
		txObject.setResourceHolder((RabbitResourceHolder) TransactionSynchronizationManager
				.getResource(getConnectionFactory()));
		return txObject;
	}

	@Override
	protected boolean isExistingTransaction(Object transaction) {
		RabbitTransactionObject txObject = (RabbitTransactionObject) transaction;
		return (txObject.getResourceHolder() != null);
	}

	@Override
	protected void doBegin(Object transaction, TransactionDefinition definition) {
		if (definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT) {
			throw new InvalidIsolationLevelException("AMQP does not support an isolation level concept");
		}
		RabbitTransactionObject txObject = (RabbitTransactionObject) transaction;
		RabbitResourceHolder resourceHolder = null;
		try {
			resourceHolder = ConnectionFactoryUtils.getTransactionalResourceHolder(getConnectionFactory(), true);
			if (logger.isDebugEnabled()) {
				logger.debug("Created AMQP transaction on channel [" + resourceHolder.getChannel() + "]");
			}
			// resourceHolder.declareTransactional();
			txObject.setResourceHolder(resourceHolder);
			txObject.getResourceHolder().setSynchronizedWithTransaction(true);
			int timeout = determineTimeout(definition);
			if (timeout != TransactionDefinition.TIMEOUT_DEFAULT) {
				txObject.getResourceHolder().setTimeoutInSeconds(timeout);
			}
			TransactionSynchronizationManager.bindResource(getConnectionFactory(), txObject.getResourceHolder());
		} catch (AmqpException ex) {
			if (resourceHolder != null) {
				ConnectionFactoryUtils.releaseResources(resourceHolder);
			}
			throw new CannotCreateTransactionException("Could not create AMQP transaction", ex);
		}
	}

	@Override
	protected Object doSuspend(Object transaction) {
		RabbitTransactionObject txObject = (RabbitTransactionObject) transaction;
		txObject.setResourceHolder(null);
		return TransactionSynchronizationManager.unbindResource(getConnectionFactory());
	}

	@Override
	protected void doResume(Object transaction, Object suspendedResources) {
		RabbitResourceHolder conHolder = (RabbitResourceHolder) suspendedResources;
		TransactionSynchronizationManager.bindResource(getConnectionFactory(), conHolder);
	}

	@Override
	protected void doCommit(DefaultTransactionStatus status) {
		RabbitTransactionObject txObject = (RabbitTransactionObject) status.getTransaction();
		RabbitResourceHolder resourceHolder = txObject.getResourceHolder();
		resourceHolder.commitAll();
	}

	@Override
	protected void doRollback(DefaultTransactionStatus status) {
		RabbitTransactionObject txObject = (RabbitTransactionObject) status.getTransaction();
		RabbitResourceHolder resourceHolder = txObject.getResourceHolder();
		resourceHolder.rollbackAll();
	}

	@Override
	protected void doSetRollbackOnly(DefaultTransactionStatus status) {
		RabbitTransactionObject txObject = (RabbitTransactionObject) status.getTransaction();
		txObject.getResourceHolder().setRollbackOnly();
	}

	@Override
	protected void doCleanupAfterCompletion(Object transaction) {
		RabbitTransactionObject txObject = (RabbitTransactionObject) transaction;
		TransactionSynchronizationManager.unbindResource(getConnectionFactory());
		txObject.getResourceHolder().closeAll();
		txObject.getResourceHolder().clear();
	}

	/**
	 * Rabbit transaction object, representing a RabbitResourceHolder. Used as transaction object by
	 * RabbitTransactionManager.
	 * @see RabbitResourceHolder
	 */
	private static class RabbitTransactionObject implements SmartTransactionObject {

		private RabbitResourceHolder resourceHolder;

		public void setResourceHolder(RabbitResourceHolder resourceHolder) {
			this.resourceHolder = resourceHolder;
		}

		public RabbitResourceHolder getResourceHolder() {
			return this.resourceHolder;
		}

		public boolean isRollbackOnly() {
			return this.resourceHolder.isRollbackOnly();
		}

		public void flush() {
			// no-op
		}
	}
}
