/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * @author Mark Fisher
 */
public abstract class RabbitAccessor implements InitializingBean {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private volatile ConnectionFactory connectionFactory;

	private volatile boolean channelTransacted;

	/**
	 * Set the ConnectionFactory to use for obtaining RabbitMQ {@link Connection Connections}.
	 */
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * Return the ConnectionFactory that this accessor uses for obtaining
	 * RabbitMQ {@link Connection Connections}.
	 */
	public ConnectionFactory getConnectionFactory() {
		return this.connectionFactory;
	}

	/**
	 * Set the transaction mode that is used for a RabbitMQ {@link Channel},
	 * Default is "false".
	 */
	public void setChannelTransacted(boolean channelTransacted) {
		this.channelTransacted = channelTransacted;
	}

	/**
	 * Return whether the RabbitMQ {@link Channel channels} used by this
	 * accessor are supposed to be transacted.
	 * @see #setChannelTransacted(boolean)
	 */
	public boolean isChannelTransacted() {
		return this.channelTransacted;
	}

	public void afterPropertiesSet() {
		Assert.notNull(this.connectionFactory, "ConnectionFactory is required");
	}

	/**
	 * Create a RabbitMQ Connection via this template's ConnectionFactory
	 * and its host and port values.
	 * @return the new RabbitMQ Connection
	 * @throws IOException if thrown by RabbitMQ API methods
	 * @see ConnectionFactory#createConnection
	 */
	protected Connection createConnection() throws IOException {
		return this.connectionFactory.createConnection();
	}
	
	/**
	 * Create a RabbitMQ Channel for the given Connection.
	 * @param con the RabbitMQ Connection to create a Channel for
	 * @return the new RabbitMQ Channel
	 * @throws IOException if thrown by RabbitMQ API methods
	 */
	protected Channel createChannel(Connection con) throws IOException {
		Assert.notNull(con, "connection must not be null");
		Channel channel = con.createChannel();
		return channel;
	}
	
	/**
	 * Fetch an appropriate Connection from the given RabbitResourceHolder.
	 * 
	 * @param holder the RabbitResourceHolder
	 * @return an appropriate Connection fetched from the holder, or
	 * <code>null</code> if none found
	 */
	protected Connection getConnection(RabbitResourceHolder holder) {
		return holder.getConnection();
	}

	/**
	 * Fetch an appropriate Channel from the given RabbitResourceHolder.
	 * 
	 * @param holder the RabbitResourceHolder
	 * @return an appropriate Channel fetched from the holder, or
	 * <code>null</code> if none found
	 */
	protected Channel getChannel(RabbitResourceHolder holder) {
		return holder.getChannel();
	}

	protected RabbitResourceHolder getTransactionalResourceHolder() {
		RabbitResourceHolder holder = ConnectionFactoryUtils.getTransactionalResourceHolder(this.connectionFactory, this.channelTransacted);
		if (isChannelTransacted()) {
			holder.declareTransactional();
		}
		return holder;
	}

	protected AmqpException convertRabbitAccessException(Exception ex) {
		return RabbitUtils.convertRabbitAccessException(ex);
	}

}
