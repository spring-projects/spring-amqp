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

package org.springframework.amqp.rabbit.core;

import java.io.IOException;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageCreator;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.support.RabbitAccessor;
import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

/**
 * Helper class that simplifies synchronous RabbitMQ access code.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
public class RabbitTemplate extends RabbitAccessor implements RabbitOperations {

	private static final String DEFAULT_EXCHANGE = ""; // alias for amq.direct default exchange

	private static final String DEFAULT_ROUTING_KEY = "";


	// TODO configure defaults
	// void basicQos(int prefetchSize, int prefetchCount, boolean global)

	private volatile String defaultExchange = DEFAULT_EXCHANGE;

	private volatile String defaultRoutingKey = DEFAULT_ROUTING_KEY;

	// The default queue name that will be used for synchronous receives.
	private volatile String defaultReceiveQueueName;

	private volatile boolean mandatoryPublish;

	private volatile boolean immediatePublish;

	private volatile boolean requireAckOnReceive = false;


	/**
	 * Internal ResourceFactory adapter for interacting with
	 * ConnectionFactoryUtils
	 */
	private final RabbitTemplateResourceFactory transactionalResourceFactory = new RabbitTemplateResourceFactory();

	private volatile MessageConverter messageConverter = new SimpleMessageConverter();


	public RabbitTemplate() {
		initDefaultStrategies();
	}

	public RabbitTemplate(ConnectionFactory connectionFactory) {
		this();
		setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}


	protected void initDefaultStrategies() {
		setMessageConverter(new SimpleMessageConverter());
	}

	public void setDefaultExchange(String exchange) {
		this.defaultExchange = (exchange != null) ? exchange : DEFAULT_EXCHANGE;
	}

	public void setDefaultRoutingKey(String defaultRoutingKey) {
		this.defaultRoutingKey = defaultRoutingKey;
	}

	public void setDefaultReceiveQueueName(String defaultQueueName) {
		this.defaultReceiveQueueName = defaultQueueName;
	}
	
	public void setDefaultReceiveQueue(Queue defaultQueue) {
		this.defaultReceiveQueueName = defaultQueue.getName();
	}

	public void setMandatoryPublish(boolean mandatoryPublish) {
		this.mandatoryPublish = mandatoryPublish;
	}

	public void setImmediatePublish(boolean immediatePublish) {
		this.immediatePublish = immediatePublish;
	}

	public void setRequireAckOnReceive(boolean requireAckOnReceive) {
		this.requireAckOnReceive = requireAckOnReceive;
	}

	/**
	 * Set the message converter for this template. Used to resolve Object
	 * parameters to convertAndSend methods and Object results from
	 * receiveAndConvert methods.
	 * <p>
	 * The default converter is a SimpleMessageConverter, which is able to
	 * handle byte arrays, Strings, and Serializable Objects.
	 * 
	 * @see #convertAndSend
	 * @see #receiveAndConvert
	 * @see org.springframework.amqp.support.converter.rabbit.support.converter.SimpleMessageConverter
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Return the message converter for this template.
	 */
	public MessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Fetch an appropriate Connection from the given RabbitResourceHolder.
	 * 
	 * @param holder
	 *            the RabbitResourceHolder
	 * @return an appropriate Connection fetched from the holder, or
	 *         <code>null</code> if none found
	 */
	protected Connection getConnection(RabbitResourceHolder holder) {
		return holder.getConnection();
	}

	/**
	 * Fetch an appropriate Channel from the given RabbitResourceHolder.
	 * 
	 * @param holder
	 *            the RabbitResourceHolder
	 * @return an appropriate Channel fetched from the holder, or
	 *         <code>null</code> if none found
	 */
	protected Channel getChannel(RabbitResourceHolder holder) {
		return holder.getChannel();
	}

	public void send(MessageCreator messageCreator) throws AmqpException {
		send(this.defaultExchange, this.defaultRoutingKey, messageCreator);
	}

	public void send(String routingKey, MessageCreator messageCreator) throws AmqpException {
		send(this.defaultExchange, routingKey, messageCreator);
	}

	public void send(final String exchange, final String routingKey, final MessageCreator messageCreator) throws AmqpException {
		execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				doSend(channel, exchange, routingKey, messageCreator);
				return null;
			}
		});
	}

	public void convertAndSend(Object object) throws AmqpException {
		convertAndSend(this.defaultExchange, this.defaultRoutingKey, object);
	}

	public void convertAndSend(String routingKey, final Object object) throws AmqpException {
		convertAndSend(this.defaultExchange, routingKey, object);
	}

	public void convertAndSend(String exchange, String routingKey, final Object object) throws AmqpException {
		send(exchange, routingKey, new MessageCreator() {
			public Message createMessage() {
				return getRequiredMessageConverter().toMessage(object, new RabbitMessageProperties());
			}
		});
	}

	public void convertAndSend(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException {
		convertAndSend(this.defaultExchange, this.defaultRoutingKey, message, messagePostProcessor);
	}

	public void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor) throws AmqpException {
		convertAndSend(this.defaultExchange, routingKey, message, messagePostProcessor);
	}

	public void convertAndSend(String exchange, String routingKey,
			final Object message,
			final MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		send(exchange, routingKey, new MessageCreator() {
			public Message createMessage() {
				Message msg = getRequiredMessageConverter().toMessage(message, new RabbitMessageProperties());
				return messagePostProcessor.postProcessMessage(msg);
			}
		});
	}

	public Message receive() throws AmqpException {
		String queueName = getRequiredDefaultQueueName();
		return this.receive(queueName);
	}

	public Message receive(final String queueName) {
		return execute(new ChannelCallback<Message>() {
			public Message doInRabbit(Channel channel) throws IOException {
				GetResponse response = channel.basicGet(queueName, !requireAckOnReceive);
				//TODO - check for null of response - got it when sending from .NET client, investigate
				if (response != null) {					
					MessageProperties messageProps = new RabbitMessageProperties(response.getProps(), 
																				 response.getEnvelope().getExchange(),
																				 response.getEnvelope().getRoutingKey(),
																				 response.getEnvelope().isRedeliver(),
																				 response.getEnvelope().getDeliveryTag(),																		   
																				 response.getMessageCount());
					return new Message(response.getBody(), messageProps);
				}
				return null;
			}
		});
	}

	public Object receiveAndConvert() throws AmqpException {
		return receiveAndConvert(getRequiredDefaultQueueName());
	}

	public Object receiveAndConvert(String queueName) throws AmqpException {
		Message response = receive(queueName);
		if (response != null) {
			return getRequiredMessageConverter().fromMessage(response);
		}
		return null;
	}

	public <T> T execute(ChannelCallback<T> action) {
		Assert.notNull(action, "Callback object must not be null");
		Connection conToClose = null;
		Channel channelToClose = null;
		try {
			Channel channelToUse = ConnectionFactoryUtils
					.doGetTransactionalChannel(getConnectionFactory(),
							this.transactionalResourceFactory);
			if (channelToUse == null) {
				conToClose = createConnection();
				channelToClose = createChannel(conToClose);
				channelToUse = channelToClose;
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Executing callback on RabbitMQ Channel: " + channelToUse);
			}
			return action.doInRabbit(channelToUse);
		}
		catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
		finally {
			RabbitUtils.closeChannel(channelToClose);
			ConnectionFactoryUtils.releaseConnection(conToClose, getConnectionFactory());
		}
	}
	

	/**
	 * Send the given message to the specified exchange.
	 * 
	 * @param channel
	 *            the RabbitMQ Channel to operate within
	 * @param exchange
	 *            the name of the RabbitMQ exchange to send to
	 * @param routingKey
	 *            the routing key to
	 * @param messageCreator
	 *            callback to create a Message
	 * @throws IOException
	 *             if thrown by RabbitMQ API methods
	 */
	protected void doSend(Channel channel,
			String exchange, String routingKey, MessageCreator messageCreator)
			throws Exception {
		Assert.notNull(messageCreator, "MessageCreator must not be null");
		try {
			Message message = messageCreator.createMessage();
			if (logger.isDebugEnabled()) {
				logger.debug("Publishing message on exchange [" + exchange + "], routingKey = [" + routingKey + "]");
			}
			
			if (exchange == null) {
				// try to send to default exchange
				exchange = this.defaultExchange;
			}

			if (routingKey == null) {
				// try to send to default routing key
				routingKey = this.defaultRoutingKey;
			}
			//TODO parameterize out default encoding				
			channel.basicPublish(exchange, routingKey,
					this.mandatoryPublish, this.immediatePublish,
					RabbitUtils.extractBasicProperties(message, "UTF-8"), message.getBody());
			// Check commit - avoid commit call within a JTA transaction.
			// TODO: should we be able to do (via wrapper) something like:
			// channel.getTransacted()?
			if (isChannelTransacted() && isChannelLocallyTransacted(channel)) {
				// Transacted channel created by this template -> commit.
				RabbitUtils.commitIfNecessary(channel);
			}
		} finally {
			// RabbitUtils. .. nothing to do here? compared to
			// session.closeMessageProducer(producer);
		}
	}

	/**
	 * Check whether the given Channel is locally transacted, that is, whether
	 * its transaction is managed by this template's Channel handling and not by
	 * an external transaction coordinator.
	 * 
	 * @param channel
	 *            the Channel to check
	 * @return whether the given Channel is locally transacted
	 * @see ConnectionFactoryUtils#isChannelTransactional
	 * @see #isChannelTransacted
	 */
	protected boolean isChannelLocallyTransacted(Channel channel) {
		return isChannelTransacted()
				&& !ConnectionFactoryUtils.isChannelTransactional(channel, getConnectionFactory());
	}

	private MessageConverter getRequiredMessageConverter() throws IllegalStateException {
		MessageConverter converter = this.getMessageConverter();
		if (converter == null) {
			throw new AmqpIllegalStateException(
					"No 'messageConverter' specified. Check configuration of RabbitTemplate.");
		}
		return converter;
	}

	private String getRequiredDefaultQueueName() throws IllegalStateException {
		String name = this.defaultReceiveQueueName;
		if (name == null) {
			throw new AmqpIllegalStateException(
					"No 'defaultQueueName' specified. Check configuration of RabbitTemplate.");
		}
		return name;
	}


	/**
	 * ResourceFactory implementation that delegates to this template's
	 * protected callback methods.
	 */
	private class RabbitTemplateResourceFactory implements ConnectionFactoryUtils.ResourceFactory {

		public Connection getConnection(RabbitResourceHolder holder) {
			return RabbitTemplate.this.getConnection(holder);
		}

		public Channel getChannel(RabbitResourceHolder holder) {
			return RabbitTemplate.this.getChannel(holder);
		}

		public Connection createConnection() throws IOException {
			return RabbitTemplate.this.createConnection();
		}

		public Channel createChannel(Connection con) throws IOException {
			return RabbitTemplate.this.createChannel(con);
		}

		public boolean isSynchedLocalTransactionAllowed() {
			return RabbitTemplate.this.isChannelTransacted();
		}
	}

}
