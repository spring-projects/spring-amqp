/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import com.rabbitmq.client.Channel;
import io.micrometer.observation.ObservationRegistry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;

/**
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
public abstract class RabbitAccessor implements InitializingBean {

	/** Logger available to subclasses. */
	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	@SuppressWarnings("NullAway.Init")
	private volatile ConnectionFactory connectionFactory;

	private volatile boolean transactional;

	private ObservationRegistry observationRegistry = ObservationRegistry.NOOP;

	public boolean isChannelTransacted() {
		return this.transactional;
	}

	/**
	 * Flag to indicate that channels created by this component will be transactional.
	 *
	 * @param transactional the flag value to set
	 */
	public void setChannelTransacted(boolean transactional) {
		this.transactional = transactional;
	}

	/**
	 * Set the ConnectionFactory to use for obtaining RabbitMQ {@link Connection Connections}.
	 *
	 * @param connectionFactory The connection factory.
	 */
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @return The ConnectionFactory that this accessor uses for obtaining RabbitMQ {@link Connection Connections}.
	 */
	public ConnectionFactory getConnectionFactory() {
		return this.connectionFactory;
	}

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(this.connectionFactory, "ConnectionFactory is required");
	}

	/**
	 * Create a RabbitMQ Connection via this template's ConnectionFactory and its host and port values.
	 * @return the new RabbitMQ Connection
	 * @see ConnectionFactory#createConnection
	 */
	protected Connection createConnection() {
		return this.connectionFactory.createConnection();
	}

	/**
	 * Fetch an appropriate Connection from the given RabbitResourceHolder.
	 *
	 * @param holder the RabbitResourceHolder
	 * @return an appropriate Connection fetched from the holder, or <code>null</code> if none found
	 */
	protected @Nullable Connection getConnection(RabbitResourceHolder holder) {
		return holder.getConnection();
	}

	/**
	 * Fetch an appropriate Channel from the given RabbitResourceHolder.
	 *
	 * @param holder the RabbitResourceHolder
	 * @return an appropriate Channel fetched from the holder, or <code>null</code> if none found
	 */
	protected @Nullable Channel getChannel(RabbitResourceHolder holder) {
		return holder.getChannel();
	}

	protected RabbitResourceHolder getTransactionalResourceHolder() {
		return ConnectionFactoryUtils.getTransactionalResourceHolder(this.connectionFactory, isChannelTransacted());
	}

	protected RuntimeException convertRabbitAccessException(Exception ex) {
		return RabbitExceptionTranslator.convertRabbitAccessException(ex);
	}

	protected void obtainObservationRegistry(@Nullable ApplicationContext appContext) {
		if (appContext != null) {
			ObjectProvider<ObservationRegistry> registry =
					appContext.getBeanProvider(ObservationRegistry.class);
			this.observationRegistry = registry.getIfUnique(() -> this.observationRegistry);
		}
	}

	protected ObservationRegistry getObservationRegistry() {
		return this.observationRegistry;
	}

}
