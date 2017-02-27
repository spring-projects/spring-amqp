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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;

/**
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 */
public abstract class RabbitAccessor implements InitializingBean {

	/** Logger available to subclasses. */
	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	private volatile ConnectionFactory connectionFactory;

	private volatile boolean transactional;

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
	public final void setConnectionFactory(ConnectionFactory connectionFactory) {
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
	 * @throws IOException if thrown by RabbitMQ API methods
	 * @see ConnectionFactory#createConnection
	 */
	protected Connection createConnection() throws IOException {
		return this.connectionFactory.createConnection();
	}

	/**
	 * Fetch an appropriate Connection from the given RabbitResourceHolder.
	 *
	 * @param holder the RabbitResourceHolder
	 * @return an appropriate Connection fetched from the holder, or <code>null</code> if none found
	 */
	protected Connection getConnection(RabbitResourceHolder holder) {
		return holder.getConnection();
	}

	/**
	 * Fetch an appropriate Channel from the given RabbitResourceHolder.
	 *
	 * @param holder the RabbitResourceHolder
	 * @return an appropriate Channel fetched from the holder, or <code>null</code> if none found
	 */
	protected Channel getChannel(RabbitResourceHolder holder) {
		return holder.getChannel();
	}

	protected RabbitResourceHolder getTransactionalResourceHolder() {
		return ConnectionFactoryUtils.getTransactionalResourceHolder(this.connectionFactory, isChannelTransacted());
	}

	protected RuntimeException convertRabbitAccessException(Exception ex) {
		return RabbitExceptionTranslator.convertRabbitAccessException(ex);
	}

}
