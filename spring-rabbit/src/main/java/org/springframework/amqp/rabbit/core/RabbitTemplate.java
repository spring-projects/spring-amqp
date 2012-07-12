/*
 * Copyright 2002-2012 the original author or authors.
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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitAccessor;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.PendingConfirm;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannel;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

/**
 * <p>
 * Helper class that simplifies synchronous RabbitMQ access (sending and receiving messages).
 * </p>
 *
 * <p>
 * The default settings are for non-transactional messaging, which reduces the amount of data exchanged with the broker.
 * To use a new transaction for every send or receive set the {@link #setChannelTransacted(boolean) channelTransacted}
 * flag. To extend the transaction over multiple invocations (more efficient), you can use a Spring transaction to
 * bracket the calls (with <code>channelTransacted=true</code> as well).
 * </p>
 *
 * <p>
 * The only mandatory property is the {@link #setConnectionFactory(ConnectionFactory) ConnectionFactory}. There are
 * strategies available for converting messages to and from Java objects (
 * {@link #setMessageConverter(MessageConverter) MessageConverter}) and for converting message headers (known as message
 * properties in AMQP, see {@link #setMessagePropertiesConverter(MessagePropertiesConverter) MessagePropertiesConverter}
 * ). The defaults probably do something sensible for typical use cases, as long as the message content-type is set
 * appropriately.
 * </p>
 *
 * <p>
 * The "send" methods all have overloaded versions that allow you to explicitly target an exchange and a routing key, or
 * you can set default values to be used in all send operations. The plain "receive" methods allow you to explicitly
 * target a queue to receive from, or you can set a default value for the template that applies to all explicit
 * receives. The convenience methods for send <b>and</b> receive use the sender defaults if no exchange or routing key
 * is specified, but they always use a temporary queue for the receive leg, so the default queue is ignored.
 * </p>
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @since 1.0
 */
public class RabbitTemplate extends RabbitAccessor implements RabbitOperations, MessageListener,
	PublisherCallbackChannel.Listener {

	private static final String DEFAULT_EXCHANGE = ""; // alias for amq.direct default exchange

	private static final String DEFAULT_ROUTING_KEY = "";

	private static final long DEFAULT_REPLY_TIMEOUT = 5000;

	private static final String DEFAULT_ENCODING = "UTF-8";

	private volatile String exchange = DEFAULT_EXCHANGE;

	private volatile String routingKey = DEFAULT_ROUTING_KEY;

	// The default queue name that will be used for synchronous receives.
	private volatile String queue;

	private volatile long replyTimeout = DEFAULT_REPLY_TIMEOUT;

	private volatile MessageConverter messageConverter = new SimpleMessageConverter();

	private volatile MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private volatile String encoding = DEFAULT_ENCODING;

	private volatile Queue replyQueue;

	private final Map<String, LinkedBlockingQueue<Message>> replyHolder = new ConcurrentHashMap<String, LinkedBlockingQueue<Message>>();

	private volatile ConfirmCallback confirmCallback;

	private volatile ReturnCallback returnCallback;

	private final Map<Object, SortedMap<Long, PendingConfirm>> pendingConfirms = new ConcurrentHashMap<Object, SortedMap<Long, PendingConfirm>>();

	private volatile boolean mandatory;

	private volatile boolean immediate;

	private final String uuid = UUID.randomUUID().toString();

	public static final String STACKED_CORRELATION_HEADER = "spring_reply_correlation";

	public static final String STACKED_REPLY_TO_HEADER = "spring_reply_to";

	/**
	 * Convenient constructor for use with setter injection. Don't forget to set the connection factory.
	 */
	public RabbitTemplate() {
		initDefaultStrategies();
	}

	/**
	 * Create a rabbit template with default strategies and settings.
	 *
	 * @param connectionFactory the connection factory to use
	 */
	public RabbitTemplate(ConnectionFactory connectionFactory) {
		this();
		setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}

	/**
	 * Set up the default strategies. Subclasses can override if necessary.
	 */
	protected void initDefaultStrategies() {
		setMessageConverter(new SimpleMessageConverter());
	}

	/**
	 * The name of the default exchange to use for send operations when none is specified. Defaults to <code>""</code>
	 * which is the default exchange in the broker (per the AMQP specification).
	 *
	 * @param exchange the exchange name to use for send operations
	 */
	public void setExchange(String exchange) {
		this.exchange = (exchange != null) ? exchange : DEFAULT_EXCHANGE;
	}

	/**
	 * The value of a default routing key to use for send operations when none is specified. Default is empty which is
	 * not helpful when using the default (or any direct) exchange, but fine if the exchange is a headers exchange for
	 * instance.
	 *
	 * @param routingKey the default routing key to use for send operations
	 */
	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}

	/**
	 * The name of the default queue to receive messages from when none is specified explicitly.
	 *
	 * @param queue the default queue name to use for receive
	 */
	public void setQueue(String queue) {
		this.queue = queue;
	}

	/**
	 * The encoding to use when inter-converting between byte arrays and Strings in message properties.
	 *
	 * @param encoding the encoding to set
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}


	/**
	 * A queue for replies; if not provided, a temporary exclusive, auto-delete queue will
	 * be used for each reply.
	 *
	 * @param replyQueue the replyQueue to set
	 */
	public void setReplyQueue(Queue replyQueue) {
		this.replyQueue = replyQueue;
	}

	/**
	 * Specify the timeout in milliseconds to be used when waiting for a reply Message when using one of the
	 * sendAndReceive methods. The default value is defined as {@link #DEFAULT_REPLY_TIMEOUT}. A negative value
	 * indicates an indefinite timeout. Not used in the plain receive methods because there is no blocking receive
	 * operation defined in the protocol.
	 *
	 * @param replyTimeout the reply timeout in milliseconds
	 *
	 * @see #sendAndReceive(String, String, Message)
	 * @see #convertSendAndReceive(String, String, Object)
	 */
	public void setReplyTimeout(long replyTimeout) {
		this.replyTimeout = replyTimeout;
	}

	/**
	 * Set the message converter for this template. Used to resolve Object parameters to convertAndSend methods and
	 * Object results from receiveAndConvert methods.
	 * <p>
	 * The default converter is a SimpleMessageConverter, which is able to handle byte arrays, Strings, and Serializable
	 * Objects depending on the message content type header.
	 *
	 * @see #convertAndSend
	 * @see #receiveAndConvert
	 * @see org.springframework.amqp.support.converter.SimpleMessageConverter
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Set the {@link MessagePropertiesConverter} for this template. This converter is used to convert between raw byte
	 * content in the message headers and plain Java objects. In particular there are limitations when dealing with very
	 * long string headers, which hopefully are rare in practice, but if you need to use long headers you might need to
	 * inject a special converter here.
	 */
	public void setMessagePropertiesConverter(MessagePropertiesConverter messagePropertiesConverter) {
		Assert.notNull(messagePropertiesConverter, "messagePropertiesConverter must not be null");
		this.messagePropertiesConverter = messagePropertiesConverter;
	}

	/**
	 * Return the message converter for this template. Useful for clients that want to take advantage of the converter
	 * in {@link ChannelCallback} implementations.
	 */
	public MessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	public void setConfirmCallback(ConfirmCallback confirmCallback) {
		Assert.state(this.confirmCallback == null || this.confirmCallback == confirmCallback,
				"Only one ConfirmCallback is supported by each RabbitTemplate");
		this.confirmCallback = confirmCallback;
	}

	public void setReturnCallback(ReturnCallback returnCallback) {
		Assert.state(this.returnCallback == null || this.returnCallback == returnCallback,
				"Only one ReturnCallback is supported by each RabbitTemplate");
		this.returnCallback = returnCallback;
	}

	public void setMandatory(boolean mandatory) {
		this.mandatory = mandatory;
	}

	public void setImmediate(boolean immediate) {
		this.immediate = immediate;
	}

	/**
	 * Gets unconfirmed correlatiom data older than age and removes them.
	 * @param age in millseconds
	 * @return the collection of correlation data for which confirms have
	 * not been received.
	 */
	public Collection<CorrelationData> getUnconfirmed(long age) {
		Set<CorrelationData> unconfirmed = new HashSet<CorrelationData>();
		synchronized (this.pendingConfirms) {
			long threshold = System.currentTimeMillis() - age;
			for (Entry<Object, SortedMap<Long, PendingConfirm>> channelPendingConfirmEntry : this.pendingConfirms.entrySet()) {
				SortedMap<Long, PendingConfirm> channelPendingConfirms = channelPendingConfirmEntry.getValue();
				Iterator<Entry<Long, PendingConfirm>> iterator = channelPendingConfirms.entrySet().iterator();
				PendingConfirm pendingConfirm;
				while (iterator.hasNext()) {
					pendingConfirm = iterator.next().getValue();
					if (pendingConfirm.getTimestamp() < threshold) {
						unconfirmed.add(pendingConfirm.getCorrelationData());
						iterator.remove();
					}
					else {
						break;
					}
				}
			}
		}
		return unconfirmed.size() > 0 ? unconfirmed : null;
	}

	public void send(Message message) throws AmqpException {
		send(this.exchange, this.routingKey, message);
	}

	public void send(String routingKey, Message message) throws AmqpException {
		send(this.exchange, routingKey, message);
	}

	public void send(final String exchange, final String routingKey, final Message message) throws AmqpException {
		this.send(exchange, routingKey, message, null);
	}

	public void send(final String exchange, final String routingKey,
			final Message message, final CorrelationData correlationData)
			throws AmqpException {
		execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				doSend(channel, exchange, routingKey, message, correlationData);
				return null;
			}
		});
	}

	public void convertAndSend(Object object) throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, object, (CorrelationData) null);
	}

	public void correlationconvertAndSend(Object object, CorrelationData correlationData) throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, object, correlationData);
	}

	public void convertAndSend(String routingKey, final Object object) throws AmqpException {
		convertAndSend(this.exchange, routingKey, object, (CorrelationData) null);
	}

	public void convertAndSend(String routingKey, final Object object, CorrelationData correlationData) throws AmqpException {
		convertAndSend(this.exchange, routingKey, object, correlationData);
	}

	public void convertAndSend(String exchange, String routingKey, final Object object) throws AmqpException {
		convertAndSend(exchange, routingKey, object, (CorrelationData) null);
	}

	public void convertAndSend(String exchange, String routingKey, final Object object, CorrelationData corrationData) throws AmqpException {
		send(exchange, routingKey, convertMessageIfNecessary(object), corrationData);
	}

	public void convertAndSend(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, message, messagePostProcessor);
	}

	public void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		convertAndSend(this.exchange, routingKey, message, messagePostProcessor, null);
	}

	public void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor,
				CorrelationData correlationData)
			throws AmqpException {
		convertAndSend(this.exchange, routingKey, message, messagePostProcessor, correlationData);
	}

	public void convertAndSend(String exchange, String routingKey, final Object message,
			final MessagePostProcessor messagePostProcessor) throws AmqpException {
		convertAndSend(exchange, routingKey, message, messagePostProcessor, null);
	}

	public void convertAndSend(String exchange, String routingKey, final Object message,
			final MessagePostProcessor messagePostProcessor, CorrelationData correlationData) throws AmqpException {
		Message messageToSend = convertMessageIfNecessary(message);
		messageToSend = messagePostProcessor.postProcessMessage(messageToSend);
		send(exchange, routingKey, messageToSend, correlationData);
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
					MessageProperties messageProps = messagePropertiesConverter.toMessageProperties(
							response.getProps(), response.getEnvelope(), encoding);
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

	public Message sendAndReceive(final Message message) throws AmqpException {
		return this.doSendAndReceive(this.exchange, this.routingKey, message);
	}

	public Message sendAndReceive(final String routingKey, final Message message) throws AmqpException {
		return this.doSendAndReceive(this.exchange, routingKey, message);
	}

	public Message sendAndReceive(final String exchange, final String routingKey, final Message message)
			throws AmqpException {
		return this.doSendAndReceive(exchange, routingKey, message);
	}

	public Object convertSendAndReceive(final Object message) throws AmqpException {
		return this.convertSendAndReceive(this.exchange, this.routingKey, message, null);
	}

	public Object convertSendAndReceive(final String routingKey, final Object message) throws AmqpException {
		return this.convertSendAndReceive(this.exchange, routingKey, message, null);
	}

	public Object convertSendAndReceive(final String exchange, final String routingKey, final Object message)
			throws AmqpException {
		return this.convertSendAndReceive(exchange, routingKey, message, null);
	}

	public Object convertSendAndReceive(final Object message, final MessagePostProcessor messagePostProcessor) throws AmqpException {
		return this.convertSendAndReceive(this.exchange, this.routingKey, message, messagePostProcessor);
	}

	public Object convertSendAndReceive(final String routingKey, final Object message, final MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		return this.convertSendAndReceive(this.exchange, routingKey, message, messagePostProcessor);
	}

	public Object convertSendAndReceive(final String exchange, final String routingKey, final Object message,
			final MessagePostProcessor messagePostProcessor) throws AmqpException {
		Message requestMessage = convertMessageIfNecessary(message);
		if (messagePostProcessor != null) {
			requestMessage = messagePostProcessor.postProcessMessage(requestMessage);
		}
		Message replyMessage = this.doSendAndReceive(exchange, routingKey, requestMessage);
		if (replyMessage == null) {
			return null;
		}
		return this.getRequiredMessageConverter().fromMessage(replyMessage);
	}

	protected Message convertMessageIfNecessary(final Object object) {
		if (object instanceof Message) {
			return (Message) object;
		}
		return getRequiredMessageConverter().toMessage(object, new MessageProperties());
	}

	/**
	 * Send a message and wait for a reply.
	 *
	 * @param exchange the exchange name
	 * @param routingKey the routing key
	 * @param message the message to send
	 * @return the message that is received in reply
	 */
	protected Message doSendAndReceive(final String exchange, final String routingKey, final Message message) {
		if (this.replyQueue == null) {
			return doSendAndReceiveWithTemporary(exchange, routingKey, message);
		}
		else {
			return doSendAndReceiveWithFixed(exchange, routingKey, message);
		}
	}

	protected Message doSendAndReceiveWithTemporary(final String exchange, final String routingKey, final Message message) {
		Message replyMessage = this.execute(new ChannelCallback<Message>() {
			public Message doInRabbit(Channel channel) throws Exception {
				final ArrayBlockingQueue<Message> replyHandoff = new ArrayBlockingQueue<Message>(1);

				Assert.isNull(message.getMessageProperties().getReplyTo(),
						"Send-and-receive methods can only be used if the Message does not already have a replyTo property.");
				DeclareOk queueDeclaration = channel.queueDeclare();
				String replyTo = queueDeclaration.getQueue();
				message.getMessageProperties().setReplyTo(replyTo);

				boolean noAck = true;
				String consumerTag = UUID.randomUUID().toString();
				boolean noLocal = true;
				boolean exclusive = true;
				DefaultConsumer consumer = new DefaultConsumer(channel) {
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
							byte[] body) throws IOException {
						MessageProperties messageProperties = messagePropertiesConverter.toMessageProperties(
								properties, envelope, encoding);
						Message reply = new Message(body, messageProperties);
						if (logger.isTraceEnabled()) {
							logger.trace("Message received " + reply);
						}
						try {
							replyHandoff.put(reply);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
				};
				channel.basicConsume(replyTo, noAck, consumerTag, noLocal, exclusive, null, consumer);
				doSend(channel, exchange, routingKey, message, null);
				Message reply = (replyTimeout < 0) ? replyHandoff.take() : replyHandoff.poll(replyTimeout,
						TimeUnit.MILLISECONDS);
				channel.basicCancel(consumerTag);
				return reply;
			}
		});
		return replyMessage;
	}

	protected Message doSendAndReceiveWithFixed(final String exchange, final String routingKey, final Message message) {
		Message replyMessage = this.execute(new ChannelCallback<Message>() {
			public Message doInRabbit(Channel channel) throws Exception {
				final LinkedBlockingQueue<Message> replyHandoff = new LinkedBlockingQueue<Message>();
				String messageTag = UUID.randomUUID().toString();
				RabbitTemplate.this.replyHolder.put(messageTag, replyHandoff);

				String replyTo = message.getMessageProperties().getReplyTo();
				if (StringUtils.hasLength(replyTo) && logger.isDebugEnabled()) {
					logger.debug("Dropping replyTo header:" + replyTo
							+ " in favor of template's configured reply-queue:"
							+ RabbitTemplate.this.replyQueue.getName());
				}
				String springReplyTo = (String) message.getMessageProperties()
						.getHeaders().get(STACKED_REPLY_TO_HEADER);
				message.getMessageProperties().setHeader(
						STACKED_REPLY_TO_HEADER,
						pushHeaderValue(replyTo,
										springReplyTo));
				message.getMessageProperties().setReplyTo(RabbitTemplate.this.replyQueue.getName());
				String correlation = (String) message.getMessageProperties()
						.getHeaders().get(STACKED_CORRELATION_HEADER);
				if (StringUtils.hasLength(correlation)) {
					message.getMessageProperties().setHeader(
							STACKED_CORRELATION_HEADER,
							pushHeaderValue(messageTag, correlation));
				} else {
					message.getMessageProperties().setHeader(
							"spring_reply_correlation", messageTag);
				}

				if (logger.isDebugEnabled()) {
					logger.debug("Sending message with tag " + messageTag);
				}
				doSend(channel, exchange, routingKey, message, null);
				Message reply = (replyTimeout < 0) ? replyHandoff.take() : replyHandoff.poll(replyTimeout,
						TimeUnit.MILLISECONDS);
				RabbitTemplate.this.replyHolder.remove(messageTag);
				return reply;
			}
		});
		return replyMessage;
	}

	public <T> T execute(ChannelCallback<T> action) {
		Assert.notNull(action, "Callback object must not be null");
		RabbitResourceHolder resourceHolder = getTransactionalResourceHolder();
		Channel channel = resourceHolder.getChannel();
		if (this.confirmCallback != null || this.returnCallback != null) {
			addListener(channel);
		}
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
	protected void doSend(Channel channel, String exchange, String routingKey, Message message,
			CorrelationData correlationData) throws Exception {
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
		if (this.confirmCallback != null && channel instanceof PublisherCallbackChannel) {
			PublisherCallbackChannel publisherCallbackChannel = (PublisherCallbackChannel) channel;
			publisherCallbackChannel.addPendingConfirm(this, channel.getNextPublishSeqNo(),
					new PendingConfirm(correlationData, System.currentTimeMillis()));
		}
		boolean mandatory = this.returnCallback == null ? false : this.mandatory;
		boolean immediate = this.returnCallback == null ? false : this.immediate;
		MessageProperties messageProperties = message.getMessageProperties();
		if (mandatory || immediate) {
			messageProperties.getHeaders().put(PublisherCallbackChannel.RETURN_CORRELATION, this.uuid);
		}
		BasicProperties convertedMessageProperties = this.messagePropertiesConverter
				.fromMessageProperties(messageProperties, encoding);
		channel.basicPublish(exchange, routingKey, mandatory, immediate,
				convertedMessageProperties, message.getBody());
		// Check if commit needed
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

	private void addListener(Channel channel) {
		if (channel instanceof PublisherCallbackChannel) {
			PublisherCallbackChannel publisherCallbackChannel = (PublisherCallbackChannel) channel;
			SortedMap<Long, PendingConfirm> pendingConfirms = publisherCallbackChannel.addListener(this);
			if (!this.pendingConfirms.containsKey(channel)) {
				this.pendingConfirms.put(channel, pendingConfirms);
				if (logger.isDebugEnabled()) {
					logger.debug("Added pending confirms for " + channel + " to map, size now " + this.pendingConfirms.size());
				}
			}
		}
		else {
			throw new IllegalStateException(
					"Channel does not support confirms or returns; " +
					"is the connection factory configured for confirms or returns?");
		}
	}

	public void handleConfirm(PendingConfirm pendingConfirm, boolean ack) {
		if (this.confirmCallback != null) {
			this.confirmCallback.confirm(pendingConfirm.getCorrelationData(), ack);
		}
		else {
			if (logger.isDebugEnabled()) {
				logger.warn("Confirm received but no callback available");
			}
		}
	}

	public void handleReturn(int replyCode,
            String replyText,
            String exchange,
            String routingKey,
            BasicProperties properties,
            byte[] body)
        throws IOException
 {
		if (this.returnCallback == null) {
			if (logger.isWarnEnabled()) {
				logger.warn("Returned message but no callback available");
			}
		}
		else {
			properties.getHeaders().remove(PublisherCallbackChannel.RETURN_CORRELATION);
			MessageProperties messageProperties = messagePropertiesConverter.toMessageProperties(
					properties, null, this.encoding);
			Message returnedMessage = new Message(body, messageProperties);
			this.returnCallback.returnedMessage(returnedMessage,
					replyCode, replyText, exchange, routingKey);
		}
	}

	public boolean isConfirmListener() {
		return this.confirmCallback != null;
	}

	public boolean isReturnListener() {
		return this.returnCallback != null;
	}

	public void removePendingConfirmsReference(Channel channel,
			SortedMap<Long, PendingConfirm> unconfirmed) {
		this.pendingConfirms.remove(channel);
		if (logger.isDebugEnabled()) {
			logger.debug("Removed pending confirms for " + channel + " from map, size now " + this.pendingConfirms.size());
		}
	}

	public String getUUID() {
		return this.uuid;
	}

	public void onMessage(Message message) {
		String messageTag = (String) message.getMessageProperties()
				.getHeaders().get(STACKED_CORRELATION_HEADER);
		if (messageTag == null) {
			logger.error("No correlation header in reply");
			return;
		}
		PoppedHeader poppedHeaderValue = popHeaderValue(messageTag);
		messageTag = poppedHeaderValue.getPoppedValue();
		message.getMessageProperties().setHeader(STACKED_CORRELATION_HEADER,
				poppedHeaderValue.getNewValue());
		String springReplyTo = (String) message.getMessageProperties()
				.getHeaders().get(STACKED_REPLY_TO_HEADER);
		if (springReplyTo != null) {
			poppedHeaderValue = popHeaderValue(springReplyTo);
			springReplyTo = poppedHeaderValue.getNewValue();
			message.getMessageProperties().setHeader(STACKED_REPLY_TO_HEADER, springReplyTo);
			message.getMessageProperties().setReplyTo(null);
		}
		LinkedBlockingQueue<Message> queue = this.replyHolder.get(messageTag);
		if (queue == null) {
			if (logger.isWarnEnabled()) {
				logger.warn("Reply received after timeout for " + messageTag);
			}
			return;
		}
		queue.add(message);
		if (logger.isDebugEnabled()) {
			logger.debug("Reply received for " + messageTag);
		}
	}

	private String pushHeaderValue(String newValue, String oldValue) {
		if (oldValue == null) {
			return newValue;
		}
		else {
			return newValue + ":" + oldValue;
		}
	}

	private PoppedHeader popHeaderValue(String value) {
		int index = value.indexOf(":");
		if (index < 0) {
			return new PoppedHeader(value, null);
		}
		else {
			return new PoppedHeader(value.substring(0, index), value.substring(index+1));
		}
	}

	private static class PoppedHeader {

		private final String poppedValue;

		private final String newValue;

		public PoppedHeader(String poppedValue, String newValue) {
			this.poppedValue = poppedValue;
			if (StringUtils.hasLength(newValue)) {
				this.newValue = newValue;
			}
			else {
				this.newValue = null;
			}
		}

		public String getPoppedValue() {
			return poppedValue;
		}

		public String getNewValue() {
			return newValue;
		}
	}

	public static interface ConfirmCallback {

		void confirm(CorrelationData correlationData, boolean ack);
	}

	public static interface ReturnCallback {

		void returnedMessage(Message message, int replyCode, String replyText,
				String exchange, String routingKey);
	}
}
