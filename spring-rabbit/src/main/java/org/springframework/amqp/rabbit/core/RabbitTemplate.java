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

package org.springframework.amqp.rabbit.core;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.ExchangeTypes;
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

	private static final String DEFAULT_EXCHANGE = ""; // alias for amq.direct
														// default exchange

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

	private volatile long replyTimeout = DEFAULT_REPLY_TIMEOUT;

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

	/**
	 * If the message doesn't get routed to a queue for any reason, the server will send an async response to let me
	 * know. Possible use case: check routing.
	 * 
	 * @param mandatoryPublish the flag value to set
	 */
	public void setMandatoryPublish(boolean mandatoryPublish) {
		this.mandatoryPublish = mandatoryPublish;
	}

	/**
	 * Like a rendezvous.
	 * 
	 * @param immediatePublish
	 */
	public void setImmediatePublish(boolean immediatePublish) {
		this.immediatePublish = immediatePublish;
	}

	/**
	 * Specify the timeout in milliseconds to be used when waiting for a reply Message when using one of the
	 * sendAndReceive methods. The default value is defined as {@link #DEFAULT_REPLY_TIMEOUT}. A negative value
	 * indicates an indefinite timeout.
	 */
	public void setReplyTimeout(long replyTimeout) {
		this.replyTimeout = replyTimeout;
	}

	/**
	 * Set the message converter for this template. Used to resolve Object parameters to convertAndSend methods and
	 * Object results from receiveAndConvert methods.
	 * <p>
	 * The default converter is a SimpleMessageConverter, which is able to handle byte arrays, Strings, and Serializable
	 * Objects.
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
		send(exchange, routingKey, getRequiredMessageConverter().toMessage(object, new MessageProperties()));
	}

	public void convertAndSend(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, message, messagePostProcessor);
	}

	public void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		convertAndSend(this.exchange, routingKey, message, messagePostProcessor);
	}

	public void convertAndSend(String exchange, String routingKey, final Object message,
			final MessagePostProcessor messagePostProcessor) throws AmqpException {
		Message messageToSend = getRequiredMessageConverter().toMessage(message, new MessageProperties());
		messageToSend = messagePostProcessor.postProcessMessage(messageToSend);
		send(exchange, routingKey, messageToSend);
	}

	public Message receive() throws AmqpException {
		String queue = this.getRequiredQueue();
		return this.receive(queue);
	}

	public Message receive(final String queueName) {
		return execute(new ChannelCallback<Message>() {
			public Message doInRabbit(Channel channel) throws IOException {
				GetResponse response = channel.basicGet(queueName, !isChannelTransacted());
				// Response can be null is the case that there is no message on the queue.
				if (response != null) {
					long deliveryTag = response.getEnvelope().getDeliveryTag();
					if (isChannelLocallyTransacted(channel)) {
						channel.basicAck(deliveryTag, false);
						channel.txCommit();
					} else if (isChannelTransacted()) {
						// Not locally transacted but it is transacted so it
						// could be synchronized with an external transaction
						ConnectionFactoryUtils.registerDeliveryTag(getConnectionFactory(), channel, deliveryTag);
					}
					MessageProperties messageProps = RabbitUtils.createMessageProperties(response.getProps(),
							response.getEnvelope(), "UTF-8");
					messageProps.setMessageCount(response.getMessageCount());
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
		return this.convertSendAndReceive(this.exchange, this.routingKey, message);
	}

	public Object convertSendAndReceive(final String routingKey, final Object message) throws AmqpException {
		return this.convertSendAndReceive(this.exchange, routingKey, message);
	}

	public Object convertSendAndReceive(final String exchange, final String routingKey, final Object message)
			throws AmqpException {
		MessageProperties messageProperties = new MessageProperties();
		Message requestMessage = getRequiredMessageConverter().toMessage(message, messageProperties);
		Message replyMessage = this.doSendAndReceive(exchange, routingKey, requestMessage);
		if (replyMessage == null) {
			return null;
		}
		return this.getRequiredMessageConverter().fromMessage(replyMessage);
	}

	private Message doSendAndReceive(final String exchange, final String routingKey, final Message message) {
		Message replyMessage = this.execute(new ChannelCallback<Message>() {
			public Message doInRabbit(Channel channel) throws Exception {
				final SynchronousQueue<Message> replyHandoff = new SynchronousQueue<Message>();

				// TODO: extract this to a method
				Address replyToAddress = message.getMessageProperties().getReplyTo();
				if (replyToAddress == null) {
					// TODO: first check for a replyToAddress property on this
					// template
					DeclareOk queueDeclaration = channel.queueDeclare();
					replyToAddress = new Address(ExchangeTypes.DIRECT, DEFAULT_EXCHANGE, queueDeclaration.getQueue());
					message.getMessageProperties().setReplyTo(replyToAddress);
				}

				boolean noAck = false;
				String consumerTag = UUID.randomUUID().toString();
				boolean noLocal = true;
				boolean exclusive = true;
				DefaultConsumer consumer = new DefaultConsumer(channel) {
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
							byte[] body) throws IOException {
						MessageProperties messageProperties = RabbitUtils.createMessageProperties(properties, envelope,
								"UTF-8");
						Message reply = new Message(body, messageProperties);
						try {
							replyHandoff.put(reply);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
				};
				channel.basicConsume(replyToAddress.getRoutingKey(), noAck, consumerTag, noLocal, exclusive, null,
						consumer);
				doSend(channel, exchange, routingKey, message);
				Message reply = (replyTimeout < 0) ? replyHandoff.take() : replyHandoff.poll(replyTimeout,
						TimeUnit.MILLISECONDS);
				channel.basicCancel(consumerTag);
				return reply;
			}
		});
		return replyMessage;
	}

	public <T> T execute(ChannelCallback<T> action) {
		Assert.notNull(action, "Callback object must not be null");
		RabbitResourceHolder resourceHolder = getTransactionalResourceHolder();
		Channel channel = resourceHolder.getChannel();
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Executing callback on RabbitMQ Channel: " + channel);
			}
			return action.doInRabbit(channel);
		} catch (Exception ex) {
			if (isChannelLocallyTransacted(channel)) {
				resourceHolder.rollbackAll();
			}
			throw convertRabbitAccessException(ex);
		} finally {
			ConnectionFactoryUtils.releaseResources(resourceHolder);
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
		// TODO parameterize out default encoding
		channel.basicPublish(exchange, routingKey, this.mandatoryPublish, this.immediatePublish,
				RabbitUtils.extractBasicProperties(message, "UTF-8"), message.getBody());
		// Check commit - avoid commit call within a JTA transaction.
		// TODO: should we be able to do (via wrapper) something like:
		// channel.getTransacted()?
		if (isChannelLocallyTransacted(channel)) {
			// Transacted channel created by this template -> commit.
			RabbitUtils.commitIfNecessary(channel);
		}
	}

	/**
	 * Check whether the given Channel is locally transacted, that is, whether its transaction is managed by this
	 * template's Channel handling and not by an external transaction coordinator.
	 * 
	 * @param channel the Channel to check
	 * @return whether the given Channel is locally transacted
	 * @see ConnectionFactoryUtils#isChannelTransactional
	 * @see #isChannelTransacted
	 */
	protected boolean isChannelLocallyTransacted(Channel channel) {
		return isChannelTransacted() && !ConnectionFactoryUtils.isChannelTransactional(channel, getConnectionFactory());
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
			throw new AmqpIllegalStateException("No 'queue' specified. Check configuration of RabbitTemplate.");
		}
		return name;
	}

}
