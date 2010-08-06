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
import java.util.UUID;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.ExchangeType;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.support.RabbitAccessor;
import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.util.Assert;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
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

	private static final long DEFAULT_REPLY_TIMEOUT = 5000;


	// TODO configure defaults
	// void basicQos(int prefetchSize, int prefetchCount, boolean global)

	private volatile String exchange = DEFAULT_EXCHANGE;

	private volatile String routingKey = DEFAULT_ROUTING_KEY;

	// The default queue name that will be used for synchronous receives.
	private volatile String queue;

	private volatile boolean mandatoryPublish;

	private volatile boolean immediatePublish;

	private volatile boolean requireAck = false;

	private volatile long replyTimeout = DEFAULT_REPLY_TIMEOUT;


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

	public void setExchange(String exchange) {
		this.exchange = (exchange != null) ? exchange : DEFAULT_EXCHANGE;
	}

	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}
	
	public void setMandatoryPublish(boolean mandatoryPublish) {
		this.mandatoryPublish = mandatoryPublish;
	}

	public void setImmediatePublish(boolean immediatePublish) {
		this.immediatePublish = immediatePublish;
	}

	public void setRequireAck(boolean requireAck) {
		this.requireAck = requireAck;
	}

	/**
	 * Specify the timeout in milliseconds to be used when waiting for
	 * a reply Message when using one of the sendAndReceive methods.
	 * The default value is defined as {@link #DEFAULT_REPLY_TIMEOUT}.
	 * A negative value indicates an indefinite timeout.
	 */
	public void setReplyTimeout(long replyTimeout) {
		this.replyTimeout = replyTimeout;
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
	 * @see org.springframework.amqp.support.converter.SimpleMessageConverter
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

	public void send(Message message) throws AmqpException {
		send(this.exchange, this.routingKey, message);
	}

	public void send(String routingKey, Message message) throws AmqpException {
		send(this.exchange, routingKey, message);
	}

	public void send(final String exchange, final String routingKey, final Message message) throws AmqpException {
		execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				doSend(channel, exchange, routingKey, message);
				return null;
			}
		});
	}

	public void convertAndSend(Object object) throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, object);
	}

	public void convertAndSend(String routingKey, final Object object) throws AmqpException {
		convertAndSend(this.exchange, routingKey, object);
	}

	public void convertAndSend(String exchange, String routingKey, final Object object) throws AmqpException {
		send(exchange, routingKey, getRequiredMessageConverter().toMessage(object, new RabbitMessageProperties()));
	}

	public void convertAndSend(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, message, messagePostProcessor);
	}

	public void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor) throws AmqpException {
		convertAndSend(this.exchange, routingKey, message, messagePostProcessor);
	}

	public void convertAndSend(String exchange, String routingKey,
			final Object message,
			final MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		Message msg = getRequiredMessageConverter().toMessage(message, new RabbitMessageProperties());
		msg = messagePostProcessor.postProcessMessage(msg); 
		send(exchange, routingKey, msg);
	}

	public Message receive() throws AmqpException {
		String queue = this.getRequiredQueue();
		return this.receive(queue);
	}

	public Message receive(final String queueName) {
		return execute(new ChannelCallback<Message>() {
			public Message doInRabbit(Channel channel) throws IOException {
				GetResponse response = channel.basicGet(queueName, !requireAck);
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
		return receiveAndConvert(this.getRequiredQueue());
	}

	public Object receiveAndConvert(String queueName) throws AmqpException {
		Message response = receive(queueName);
		if (response != null) {
			return getRequiredMessageConverter().fromMessage(response);
		}
		return null;
	}

	public Object convertSendAndReceive(final Object message) throws AmqpException {
		RabbitMessageProperties messageProperties = new RabbitMessageProperties();
		Message requestMessage = getRequiredMessageConverter().toMessage(message, messageProperties);
		Message replyMessage = this.doSendAndReceive(requestMessage);
		if (replyMessage == null) {
			return null;
		}
		return this.getRequiredMessageConverter().fromMessage(replyMessage);
	}

	private Message doSendAndReceive(final Message message) {
		Message replyMessage = this.execute(new ChannelCallback<Message>() {
			public Message doInRabbit(Channel channel) throws Exception {
				final SynchronousQueue<Message> replyHandoff = new SynchronousQueue<Message>();

				// TODO: extract this to a method
				Address replyToAddress = message.getMessageProperties().getReplyTo();
				if (replyToAddress == null) {
					// TODO: first check for a replyToAddress property on this template
					DeclareOk queueDeclaration = channel.queueDeclare();
					replyToAddress = new Address(ExchangeType.direct, DEFAULT_EXCHANGE, queueDeclaration.getQueue());
					message.getMessageProperties().setReplyTo(replyToAddress);
				}

				boolean noAck = false;
				String consumerTag = UUID.randomUUID().toString();
				boolean noLocal = true;
				boolean exclusive = true;
				DefaultConsumer consumer = new DefaultConsumer(channel) {
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
							throws IOException {
						MessageProperties messageProperties = new RabbitMessageProperties(properties,
								envelope.getExchange(), envelope.getRoutingKey(), envelope.isRedeliver(), envelope.getDeliveryTag(), 0);
						Message reply = new Message(body, messageProperties);
						try {
							replyHandoff.put(reply);
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
				};
				channel.basicConsume(replyToAddress.getRoutingKey(), noAck, consumerTag, noLocal, exclusive, consumer);
				// TODO: get exchange and routing key from method args
				//       methods that are higher in the stack can determine whether to fallback to template properties
				doSend(channel, exchange, routingKey, message);
				Message reply = (replyTimeout < 0) ? replyHandoff.take()
						: replyHandoff.poll(replyTimeout, TimeUnit.MILLISECONDS);
				channel.basicCancel(consumerTag);
				return reply;
			}
		});
		return replyMessage;
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
	 * @param channel the RabbitMQ Channel to operate within
	 * @param exchange the name of the RabbitMQ exchange to send to
	 * @param routingKey the routing key
	 * @param message the Message to send
	 * @throws IOException if thrown by RabbitMQ API methods
	 */
	private void doSend(Channel channel, String exchange, String routingKey, Message message) throws Exception {
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Publishing message on exchange [" + exchange + "], routingKey = [" + routingKey + "]");
			}
			
			if (exchange == null) {
				// try to send to configured exchange
				exchange = this.exchange;
			}

			if (routingKey == null) {
				// try to send to configured routing key
				routingKey = this.routingKey;
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

	private String getRequiredQueue() throws IllegalStateException {
		String name = this.queue;
		if (name == null) {
			throw new AmqpIllegalStateException(
					"No 'queue' specified. Check configuration of RabbitTemplate.");
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
