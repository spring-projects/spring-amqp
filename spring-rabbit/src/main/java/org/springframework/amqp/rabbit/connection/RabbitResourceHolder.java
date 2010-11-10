/*
 * Copyright 2002-2010 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.connection;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.transaction.support.ResourceHolderSupport;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * Rabbit resource holder, wrapping a RabbitMQ Connection and Channel. RabbitTransactionManager binds instances of this
 * class to the thread, for a given Rabbit ConnectionFactory.
 * 
 * <p>
 * Note: This is an SPI class, not intended to be used by applications.
 * 
 * @author Mark Fisher
 * @see RabbitTransactionManager (not yet implemented)
 * @see RabbitTemplate
 */
public class RabbitResourceHolder extends ResourceHolderSupport {

	private static final Log logger = LogFactory.getLog(RabbitResourceHolder.class);

	private boolean frozen = false;

	private final List<Connection> connections = new LinkedList<Connection>();

	private final List<Channel> channels = new LinkedList<Channel>();

	private final Map<Connection, List<Channel>> channelsPerConnection = new HashMap<Connection, List<Channel>>();

	private MultiValueMap<Channel, Long> deliveryTags = new LinkedMultiValueMap<Channel, Long>();

	private boolean transactional;

	/**
	 * Create a new RabbitResourceHolder that is open for resources to be added.
	 */
	public RabbitResourceHolder() {
	}

	/**
	 * @param channel a channel to add
	 */
	public RabbitResourceHolder(Channel channel) {
		this();
		addChannel(channel);
	}

	public final boolean isFrozen() {
		return this.frozen;
	}

	public final void addConnection(Connection connection) {
		Assert.isTrue(!this.frozen, "Cannot add Connection because RabbitResourceHolder is frozen");
		Assert.notNull(connection, "Connection must not be null");
		if (!this.connections.contains(connection)) {
			this.connections.add(connection);
		}
	}

	public final void addChannel(Channel channel) {
		addChannel(channel, null);
	}

	public final void addChannel(Channel channel, Connection connection) {
		Assert.isTrue(!this.frozen, "Cannot add Channel because RabbitResourceHolder is frozen");
		Assert.notNull(channel, "Channel must not be null");
		if (!this.channels.contains(channel)) {
			this.channels.add(channel);
			if (connection != null) {
				List<Channel> channels = this.channelsPerConnection.get(connection);
				if (channels == null) {
					channels = new LinkedList<Channel>();
					this.channelsPerConnection.put(connection, channels);
				}
				channels.add(channel);
			}
		}
	}

	public boolean containsChannel(Channel channel) {
		return this.channels.contains(channel);
	}

	public Connection getConnection() {
		return (!this.connections.isEmpty() ? this.connections.get(0) : null);
	}

	public Connection getConnection(Class<? extends Connection> connectionType) {
		return CollectionUtils.findValueOfType(this.connections, connectionType);
	}

	public Channel getChannel() {
		return (!this.channels.isEmpty() ? this.channels.get(0) : null);
	}

	public void commitAll() throws IOException {
		for (Channel channel : this.channels) {
			if (deliveryTags.containsKey(channel)) {
				for (Long deliveryTag : deliveryTags.get(channel)) {
					channel.basicAck(deliveryTag, false);
				}
			}
			channel.txCommit();
		}
	}

	public void closeAll() {
		for (Channel channel : this.channels) {
			try {
				channel.close();
			} catch (Throwable ex) {
				logger.debug("Could not close synchronized Rabbit Channel after transaction", ex);
			}
		}
		for (Connection con : this.connections) {
			ConnectionFactoryUtils.releaseConnection(con);
		}
		this.connections.clear();
		this.channels.clear();
		this.channelsPerConnection.clear();
	}

	public void addDeliveryTag(Channel channel, long deliveryTag) {
		this.deliveryTags.add(channel, deliveryTag);
	}

	public void rollbackAll() {
		for (Channel channel : this.channels) {
			if (logger.isDebugEnabled()) {
				logger.debug("Rolling back messages to channel: " + channel);
			}
			RabbitUtils.rollbackIfNecessary(channel);
			if (deliveryTags.containsKey(channel)) {
				for (Long deliveryTag : deliveryTags.get(channel)) {
					try {
						channel.basicReject(deliveryTag, true);
						// Need to commit the reject (=nack)
						RabbitUtils.commitIfNecessary(channel);
					} catch (IOException ex) {
						throw new AmqpIOException(ex);
					}
				}
			}
		}
	}

	/**
	 * Call this method (with true argument) once the channel has had {@link Channel#txSelect()} has been called
	 * @param transactional the flag to set
	 */
	public void declareTransactional() {
		if (!isSynchronizedWithTransaction() && !transactional) {
			for (Channel channel : this.channels) {
				RabbitUtils.declareTransactional(channel);
			}
			this.transactional = !channels.isEmpty();
		}
	}

	/**
	 * @return true if the channels in this holder are transactional
	 */
	public boolean isChannelTransactional() {
		return this.transactional;
	}

}
