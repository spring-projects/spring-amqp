/*
 * Copyright 2002-2015 the original author or authors.
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
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
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
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.ReceiveAndReplyCallback;
import org.springframework.amqp.core.ReceiveAndReplyMessageCallback;
import org.springframework.amqp.core.ReplyToAddressCallback;
import org.springframework.amqp.rabbit.connection.AbstractRoutingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
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
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.amqp.rabbit.support.ValueExpression;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.postprocessor.MessagePostProcessorUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
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
 * @author Artem Bilan
 * @since 1.0
 */
public class RabbitTemplate extends RabbitAccessor
		implements BeanFactoryAware, RabbitOperations, MessageListener, PublisherCallbackChannel.Listener {

	/** Alias for amq.direct default exchange */
	private static final String DEFAULT_EXCHANGE = "";

	private static final String DEFAULT_ROUTING_KEY = "";

	private static final long DEFAULT_REPLY_TIMEOUT = 5000;

	private static final String DEFAULT_ENCODING = "UTF-8";

	private final ConcurrentHashMap<Object, SortedMap<Long, PendingConfirm>> pendingConfirms =
			new ConcurrentHashMap<Object, SortedMap<Long, PendingConfirm>>();

	private final Map<String, PendingReply> replyHolder = new ConcurrentHashMap<String, PendingReply>();

	private final String uuid = UUID.randomUUID().toString();

	private final StandardEvaluationContext evaluationContext = new StandardEvaluationContext();

	private final ReplyToAddressCallback<?> defaultReplyToAddressCallback = new ReplyToAddressCallback<Object>() {

		@Override
		public Address getReplyToAddress(Message request, Object reply) {
			return RabbitTemplate.this.getReplyToAddress(request);
		}

	};

	private volatile String exchange = DEFAULT_EXCHANGE;

	private volatile String routingKey = DEFAULT_ROUTING_KEY;

	// The default queue name that will be used for synchronous receives.
	private volatile String queue;

	private volatile long replyTimeout = DEFAULT_REPLY_TIMEOUT;

	private volatile MessageConverter messageConverter = new SimpleMessageConverter();

	private volatile MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private volatile String encoding = DEFAULT_ENCODING;

	private volatile String replyAddress;

	private volatile ConfirmCallback confirmCallback;

	private volatile ReturnCallback returnCallback;

	private volatile Expression mandatoryExpression = new ValueExpression<Boolean>(false);

	private volatile String correlationKey = null;

	private volatile RetryTemplate retryTemplate;

	private volatile RecoveryCallback<?> recoveryCallback;

	private volatile Expression sendConnectionFactorySelectorExpression;

	private volatile Expression receiveConnectionFactorySelectorExpression;

	private volatile boolean usingFastReplyTo;

	private volatile boolean evaluatedFastReplyTo;

	private volatile Collection<MessagePostProcessor> beforePublishPostProcessors;

	private volatile Collection<MessagePostProcessor> afterReceivePostProcessors;

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
	 * be used for each reply, unless RabbitMQ supports 'amq.rabbitmq.reply-to' - see
	 * http://www.rabbitmq.com/direct-reply-to.html
	 * @deprecated - use #setReplyAddress(String replyAddress)
	 * @param replyQueue the replyQueue to set
	 */
	@Deprecated
	public void setReplyQueue(Queue replyQueue) {
		setReplyAddress(replyQueue.getName());
	}

	/**
	 * An address for replies; if not provided, a temporary exclusive, auto-delete queue will
	 * be used for each reply, unless RabbitMQ supports 'amq.rabbitmq.reply-to' - see
	 * http://www.rabbitmq.com/direct-reply-to.html
	 * <p>The address can be a simple queue name (in which case the reply will be routed via the default
	 * exchange), or with the form {@code exchange/routingKey} to route the reply using an explicit
	 * exchange and routing key.
	 * @param replyAddress the replyAddress to set
	 */
	public void setReplyAddress(String replyAddress) {
		this.replyAddress = replyAddress;
		this.evaluatedFastReplyTo = false;
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
	 * @param messageConverter The message converter.
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
	 *
	 * @param messagePropertiesConverter The message properties converter.
	 */
	public void setMessagePropertiesConverter(MessagePropertiesConverter messagePropertiesConverter) {
		Assert.notNull(messagePropertiesConverter, "messagePropertiesConverter must not be null");
		this.messagePropertiesConverter = messagePropertiesConverter;
	}

	/**
	 * Return the message converter for this template. Useful for clients that want to take advantage of the converter
	 * in {@link ChannelCallback} implementations.
	 *
	 * @return The message converter.
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
		this.mandatoryExpression = new ValueExpression<Boolean>(mandatory);
	}

	/**
	 * @param mandatoryExpression a SpEL {@link Expression} to evaluate against each request
	 * message, if a {@link #returnCallback} has been provided. The result of expression must be
	 * a {@code boolean} value.
	 * @since 1.4
	 */
	public void setMandatoryExpression(Expression mandatoryExpression) {
		Assert.notNull(mandatoryExpression, "'mandatoryExpression' must not be null");
		this.mandatoryExpression = mandatoryExpression;
	}

	/**
	 * A SpEL {@link Expression} to evaluate
	 * against each request message, if the provided {@link #getConnectionFactory()}
	 * is an instance of {@link AbstractRoutingConnectionFactory}.
	 * <p>
	 * The result of this expression is used as {@code lookupKey} to get the target
	 * {@link ConnectionFactory} from {@link AbstractRoutingConnectionFactory}
	 * directly.
	 * <p>
	 * If this expression is evaluated to {@code null}, we fallback to the normal
	 * {@link AbstractRoutingConnectionFactory} logic.
	 * <p>
	 * If there is no target {@link ConnectionFactory} with the evaluated {@code lookupKey},
	 * we fallback to the normal {@link AbstractRoutingConnectionFactory} logic
	 * only if its property {@code lenientFallback == true}.
	 * <p>
	 *  This expression is used for {@code send} operations.
	 * @param sendConnectionFactorySelectorExpression a SpEL {@link Expression} to evaluate
	 * @since 1.4
	 */
	public void setSendConnectionFactorySelectorExpression(Expression sendConnectionFactorySelectorExpression) {
		this.sendConnectionFactorySelectorExpression = sendConnectionFactorySelectorExpression;
	}

	/**
	 * A SpEL {@link Expression} to evaluate
	 * against each {@code receive} {@code queueName}, if the provided {@link #getConnectionFactory()}
	 * is an instance of {@link AbstractRoutingConnectionFactory}.
	 * <p>
	 * The result of this expression is used as {@code lookupKey} to get the target
	 * {@link ConnectionFactory} from {@link AbstractRoutingConnectionFactory}
	 * directly.
	 * <p>
	 * If this expression is evaluated to {@code null}, we fallback to the normal
	 * {@link AbstractRoutingConnectionFactory} logic.
	 * <p>
	 * If there is no target {@link ConnectionFactory} with the evaluated {@code lookupKey},
	 * we fallback to the normal {@link AbstractRoutingConnectionFactory} logic
	 * only if its property {@code lenientFallback == true}.
	 * <p>
	 *  This expression is used for {@code receive} operations.
	 * @param receiveConnectionFactorySelectorExpression a SpEL {@link Expression} to evaluate
	 * @since 1.4
	 */
	public void setReceiveConnectionFactorySelectorExpression(Expression receiveConnectionFactorySelectorExpression) {
		this.receiveConnectionFactorySelectorExpression = receiveConnectionFactorySelectorExpression;
	}

	/**
	 * If set to 'correlationId' (default) the correlationId property
	 * will be used; otherwise the supplied key will be used.
	 * @param correlationKey the correlationKey to set
	 */
	public void setCorrelationKey(String correlationKey) {
		Assert.hasText(correlationKey, "'correlationKey' must not be null or empty");
		if (!correlationKey.trim().equals("correlationId")) {
			this.correlationKey = correlationKey.trim();
		}
	}

	/**
	 * Add a {@link RetryTemplate} which will be used for all rabbit operations.
	 * @param retryTemplate The retry template.
	 */
	public void setRetryTemplate(RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
	}

	/**
	 * Add a {@link RecoveryCallback} which is used for the {@code retryTemplate.execute}.
	 * If {@link #retryTemplate} isn't provided {@link #recoveryCallback} is ignored.
	 * {@link RecoveryCallback} should produce result compatible with
	 * {@link #execute(ChannelCallback, ConnectionFactory)} return type.
	 * @param recoveryCallback The retry recoveryCallback.
	 * @since 1.4
	 */
	public void setRecoveryCallback(RecoveryCallback<?> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.evaluationContext.setBeanResolver(new BeanFactoryResolver(beanFactory));
		this.evaluationContext.addPropertyAccessor(new MapAccessor());
	}

	/**
	 * Set {@link MessagePostProcessor}s that will be invoked immediately before invoking
	 * {@code Channel#basicPublish()}, after all other processing, except creating the
	 * {@link BasicProperties} from {@link MessageProperties}. May be used for operations
	 * such as compression. Processors are invoked in order, depending on {@code PriorityOrder},
	 * {@code Order} and finally unordered.
	 * @param beforePublishPostProcessors the post processor.
	 * @since 1.4.2
	 */
	public void setBeforePublishPostProcessors(MessagePostProcessor... beforePublishPostProcessors) {
		Assert.notNull(beforePublishPostProcessors, "'beforePublishPostProcessors' cannot be null");
		Assert.noNullElements(beforePublishPostProcessors, "'beforePublishPostProcessors' cannot have null elements");
		this.beforePublishPostProcessors = MessagePostProcessorUtils.sort(Arrays.asList(beforePublishPostProcessors));
	}

	/**
	 * @deprecated use {@link #setAfterReceivePostProcessors(MessagePostProcessor...)}
	 * @param afterReceivePostProcessors the post processors.
	 * @since 1.4.2
	 */
	@Deprecated
	public void setAfterReceivePostProcessor(MessagePostProcessor... afterReceivePostProcessors) {
		setAfterReceivePostProcessors(afterReceivePostProcessors);
	}

	/**
	 * Set a {@link MessagePostProcessor} that will be invoked immediately after a {@code Channel#basicGet()}
	 * and before any message conversion is performed.
	 * May be used for operations such as decompression  Processors are invoked in order,
	 * depending on {@code PriorityOrder}, {@code Order} and finally unordered.
	 * @param afterReceivePostProcessors the post processor.
	 * @since 1.5
	 */
	public void setAfterReceivePostProcessors(MessagePostProcessor... afterReceivePostProcessors) {
		Assert.notNull(afterReceivePostProcessors, "'afterReceivePostProcessors' cannot be null");
		Assert.noNullElements(afterReceivePostProcessors, "'afterReceivePostProcessors' cannot have null elements");
		this.afterReceivePostProcessors = MessagePostProcessorUtils.sort(Arrays.asList(afterReceivePostProcessors));
	}

	/**
	 * Gets unconfirmed correlation data older than age and removes them.
	 * @param age in milliseconds
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

	private void evaluateFastReplyTo() {
		this.usingFastReplyTo = false;
		if (this.replyAddress == null || Address.AMQ_RABBITMQ_REPLY_TO.equals(this.replyAddress)) {
			try {
				execute(new ChannelCallback<Void>() {

					@Override
					public Void doInRabbit(Channel channel) throws Exception {
						channel.queueDeclarePassive(Address.AMQ_RABBITMQ_REPLY_TO);
						return null;
					}
				});
				this.usingFastReplyTo = true;
			}
			catch (Exception e) {
				if (this.replyAddress != null) {
					logger.error("Broker does not support fast replies via 'amq.rabbitmq.reply-to', temporary "
							+ "queues will be used:" + e.getMessage() + ".");
				}
				else {
					if (logger.isDebugEnabled()) {
						logger.debug("Broker does not support fast replies via 'amq.rabbitmq.reply-to', temporary "
								+ "queues will be used:" + e.getMessage() + ".");
					}
				}
				this.replyAddress = null;
			}
		}
		this.evaluatedFastReplyTo = true;
	}

	@Override
	public void send(Message message) throws AmqpException {
		send(this.exchange, this.routingKey, message);
	}

	@Override
	public void send(String routingKey, Message message) throws AmqpException {
		send(this.exchange, routingKey, message);
	}

	@Override
	public void send(final String exchange, final String routingKey, final Message message) throws AmqpException {
		this.send(exchange, routingKey, message, null);
	}

	public void send(final String exchange, final String routingKey,
			final Message message, final CorrelationData correlationData)
			throws AmqpException {
		execute(new ChannelCallback<Object>() {

			@Override
			public Object doInRabbit(Channel channel) throws Exception {
				doSend(channel, exchange, routingKey, message, correlationData);
				return null;
			}
		}, obtainTargetConnectionFactoryIfNecessary(this.sendConnectionFactorySelectorExpression, message));
	}

	private ConnectionFactory obtainTargetConnectionFactoryIfNecessary(Expression expression, Object rootObject) {
		if (expression != null && getConnectionFactory() instanceof AbstractRoutingConnectionFactory) {
			AbstractRoutingConnectionFactory routingConnectionFactory =
					(AbstractRoutingConnectionFactory) getConnectionFactory();
			Object lookupKey = null;
			if (rootObject != null) {
				lookupKey = this.sendConnectionFactorySelectorExpression.getValue(this.evaluationContext, rootObject);
			}
			else {
				lookupKey = this.sendConnectionFactorySelectorExpression.getValue(this.evaluationContext);
			}
			if (lookupKey != null) {
				ConnectionFactory connectionFactory = routingConnectionFactory.getTargetConnectionFactory(lookupKey);
				if (connectionFactory != null) {
					return connectionFactory;
				}
				else if (!routingConnectionFactory.isLenientFallback()) {
					throw new IllegalStateException("Cannot determine target ConnectionFactory for lookup key ["
							+ lookupKey + "]");
				}
			}
		}
		return null;
	}

	@Override
	public void convertAndSend(Object object) throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, object, (CorrelationData) null);
	}

	@Deprecated
	public void correlationconvertAndSend(Object object, CorrelationData correlationData) throws AmqpException {
		this.correlationConvertAndSend(object, correlationData);
	}


	public void correlationConvertAndSend(Object object, CorrelationData correlationData) throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, object, correlationData);
	}

	@Override
	public void convertAndSend(String routingKey, final Object object) throws AmqpException {
		convertAndSend(this.exchange, routingKey, object, (CorrelationData) null);
	}

	public void convertAndSend(String routingKey, final Object object, CorrelationData correlationData) throws AmqpException {
		convertAndSend(this.exchange, routingKey, object, correlationData);
	}

	@Override
	public void convertAndSend(String exchange, String routingKey, final Object object) throws AmqpException {
		convertAndSend(exchange, routingKey, object, (CorrelationData) null);
	}

	public void convertAndSend(String exchange, String routingKey, final Object object, CorrelationData correlationData) throws AmqpException {
		send(exchange, routingKey, convertMessageIfNecessary(object), correlationData);
	}

	@Override
	public void convertAndSend(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, message, messagePostProcessor);
	}

	@Override
	public void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		convertAndSend(this.exchange, routingKey, message, messagePostProcessor, null);
	}

	public void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor,
				CorrelationData correlationData)
			throws AmqpException {
		convertAndSend(this.exchange, routingKey, message, messagePostProcessor, correlationData);
	}

	@Override
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

	@Override
	public Message receive() throws AmqpException {
		String queue = this.getRequiredQueue();
		return this.receive(queue);
	}

	@Override
	public Message receive(final String queueName) {
		return execute(new ChannelCallback<Message>() {

			@Override
			public Message doInRabbit(Channel channel) throws IOException {
				GetResponse response = channel.basicGet(queueName, !isChannelTransacted());
				// Response can be null is the case that there is no message on the queue.
				if (response != null) {
					long deliveryTag = response.getEnvelope().getDeliveryTag();
					if (isChannelLocallyTransacted(channel)) {
						channel.basicAck(deliveryTag, false);
						channel.txCommit();
					}
					else if (isChannelTransacted()) {
						// Not locally transacted but it is transacted so it
						// could be synchronized with an external transaction
						ConnectionFactoryUtils.registerDeliveryTag(getConnectionFactory(), channel, deliveryTag);
					}

					return RabbitTemplate.this.buildMessageFromResponse(response);
				}
				return null;
			}
		}, obtainTargetConnectionFactoryIfNecessary(this.receiveConnectionFactorySelectorExpression, queueName));
	}

	@Override
	public Object receiveAndConvert() throws AmqpException {
		return receiveAndConvert(this.getRequiredQueue());
	}

	@Override
	public Object receiveAndConvert(String queueName) throws AmqpException {
		Message response = receive(queueName);
		if (response != null) {
			return getRequiredMessageConverter().fromMessage(response);
		}
		return null;
	}

	@Override
	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback) throws AmqpException {
		return this.receiveAndReply(this.getRequiredQueue(), callback);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <R, S> boolean receiveAndReply(final String queueName, ReceiveAndReplyCallback<R, S> callback) throws AmqpException {
		return this.receiveAndReply(queueName, callback, (ReplyToAddressCallback<S>) this.defaultReplyToAddressCallback);

	}

	@Override
	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback, final String exchange, final String routingKey)
			throws AmqpException {
		return this.receiveAndReply(this.getRequiredQueue(), callback, exchange, routingKey);
	}

	@Override
	public <R, S> boolean receiveAndReply(final String queueName, ReceiveAndReplyCallback<R, S> callback, final String replyExchange,
										  final String replyRoutingKey) throws AmqpException {
		return this.receiveAndReply(queueName, callback, new ReplyToAddressCallback<S>() {

			@Override
			public Address getReplyToAddress(Message request, S reply) {
				return new Address(replyExchange, replyRoutingKey);
			}

		});
	}

	@Override
	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback, ReplyToAddressCallback<S> replyToAddressCallback)
			throws AmqpException {
		return this.receiveAndReply(this.getRequiredQueue(), callback, replyToAddressCallback);
	}

	@Override
	public <R, S> boolean receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback,
										  ReplyToAddressCallback<S> replyToAddressCallback) throws AmqpException {
		return this.doReceiveAndReply(queueName, callback, replyToAddressCallback);
	}

	@SuppressWarnings("unchecked")
	private <R, S> boolean doReceiveAndReply(final String queueName, final ReceiveAndReplyCallback<R, S> callback,
											 final ReplyToAddressCallback<S> replyToAddressCallback) throws AmqpException {
		return this.execute(new ChannelCallback<Boolean>() {

			@Override
			public Boolean doInRabbit(Channel channel) throws Exception {
				boolean channelTransacted = RabbitTemplate.this.isChannelTransacted();

				GetResponse response = channel.basicGet(queueName, !channelTransacted);
				// Response can be null in the case that there is no message on the queue.
				if (response != null) {
					long deliveryTag = response.getEnvelope().getDeliveryTag();
					boolean channelLocallyTransacted = RabbitTemplate.this.isChannelLocallyTransacted(channel);

					if (channelLocallyTransacted) {
						channel.basicAck(deliveryTag, false);
					}
					else if (channelTransacted) {
						// Not locally transacted but it is transacted so it could be synchronized with an external transaction
						ConnectionFactoryUtils.registerDeliveryTag(RabbitTemplate.this.getConnectionFactory(), channel, deliveryTag);
					}

					Message receiveMessage = RabbitTemplate.this.buildMessageFromResponse(response);

					Object receive = receiveMessage;
					if (!(ReceiveAndReplyMessageCallback.class.isAssignableFrom(callback.getClass()))) {
						receive = RabbitTemplate.this.getRequiredMessageConverter().fromMessage(receiveMessage);
					}

					S reply;
					try {
						reply = callback.handle((R) receive);
					}
					catch (ClassCastException e) {
						StackTraceElement[] trace = e.getStackTrace();
						if (trace[0].getMethodName().equals("handle") && trace[1].getFileName().equals("RabbitTemplate.java")) {
							throw new IllegalArgumentException("ReceiveAndReplyCallback '" + callback
									+ "' can't handle received object '" + receive + "'", e);
						}
						else {
							throw e;
						}
					}

					if (reply != null) {
						Address replyTo = replyToAddressCallback.getReplyToAddress(receiveMessage, reply);

						Message replyMessage = RabbitTemplate.this.convertMessageIfNecessary(reply);

						MessageProperties receiveMessageProperties = receiveMessage.getMessageProperties();
						MessageProperties replyMessageProperties = replyMessage.getMessageProperties();

						Object correlation = RabbitTemplate.this.correlationKey == null
								? receiveMessageProperties.getCorrelationId()
								: receiveMessageProperties.getHeaders().get(RabbitTemplate.this.correlationKey);

						if (RabbitTemplate.this.correlationKey == null || correlation == null) {
							// using standard correlationId property
							if (correlation == null) {
								String messageId = receiveMessageProperties.getMessageId();
								if (messageId != null) {
									correlation = messageId.getBytes(RabbitTemplate.this.encoding);
								}
							}
							replyMessageProperties.setCorrelationId((byte[]) correlation);
						}
						else {
							replyMessageProperties.setHeader(RabbitTemplate.this.correlationKey, correlation);
						}

						// 'doSend()' takes care about 'channel.txCommit()'.
						RabbitTemplate.this.doSend(channel, replyTo.getExchangeName(), replyTo.getRoutingKey(), replyMessage, null);
					}
					else if (channelLocallyTransacted) {
						channel.txCommit();
					}

					return true;
				}
				return false;
			}
		}, obtainTargetConnectionFactoryIfNecessary(this.receiveConnectionFactorySelectorExpression, queueName));
	}

	@Override
	public Message sendAndReceive(final Message message) throws AmqpException {
		return this.doSendAndReceive(this.exchange, this.routingKey, message);
	}

	@Override
	public Message sendAndReceive(final String routingKey, final Message message) throws AmqpException {
		return this.doSendAndReceive(this.exchange, routingKey, message);
	}

	@Override
	public Message sendAndReceive(final String exchange, final String routingKey, final Message message)
			throws AmqpException {
		return this.doSendAndReceive(exchange, routingKey, message);
	}

	@Override
	public Object convertSendAndReceive(final Object message) throws AmqpException {
		return this.convertSendAndReceive(this.exchange, this.routingKey, message, null);
	}

	@Override
	public Object convertSendAndReceive(final String routingKey, final Object message) throws AmqpException {
		return this.convertSendAndReceive(this.exchange, routingKey, message, null);
	}

	@Override
	public Object convertSendAndReceive(final String exchange, final String routingKey, final Object message)
			throws AmqpException {
		return this.convertSendAndReceive(exchange, routingKey, message, null);
	}

	@Override
	public Object convertSendAndReceive(final Object message, final MessagePostProcessor messagePostProcessor) throws AmqpException {
		return this.convertSendAndReceive(this.exchange, this.routingKey, message, messagePostProcessor);
	}

	@Override
	public Object convertSendAndReceive(final String routingKey, final Object message, final MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		return this.convertSendAndReceive(this.exchange, routingKey, message, messagePostProcessor);
	}

	@Override
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
		if (!this.evaluatedFastReplyTo) {
			synchronized(this) {
				if (!this.evaluatedFastReplyTo) {
					evaluateFastReplyTo();
				}
			}
		}
		if (this.replyAddress == null || this.usingFastReplyTo) {
			return doSendAndReceiveWithTemporary(exchange, routingKey, message);
		}
		else {
			return doSendAndReceiveWithFixed(exchange, routingKey, message);
		}
	}

	protected Message doSendAndReceiveWithTemporary(final String exchange, final String routingKey, final Message message) {
		return this.execute(new ChannelCallback<Message>() {

			@Override
			public Message doInRabbit(Channel channel) throws Exception {
				final ArrayBlockingQueue<Message> replyHandoff = new ArrayBlockingQueue<Message>(1);

				Assert.isNull(message.getMessageProperties().getReplyTo(),
						"Send-and-receive methods can only be used if the Message does not already have a replyTo property.");
				String replyTo;
				if (RabbitTemplate.this.usingFastReplyTo) {
					replyTo = Address.AMQ_RABBITMQ_REPLY_TO;
				}
				else {
					DeclareOk queueDeclaration = channel.queueDeclare();
					replyTo = queueDeclaration.getQueue();
				}
				message.getMessageProperties().setReplyTo(replyTo);

				String consumerTag = UUID.randomUUID().toString();
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
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
				};
				channel.basicConsume(replyTo, true, consumerTag, true, true, null, consumer);
				doSend(channel, exchange, routingKey, message, null);
				Message reply = (replyTimeout < 0) ? replyHandoff.take() : replyHandoff.poll(replyTimeout,
						TimeUnit.MILLISECONDS);
				channel.basicCancel(consumerTag);
				return reply;
			}
		}, obtainTargetConnectionFactoryIfNecessary(this.sendConnectionFactorySelectorExpression, message));
	}

	protected Message doSendAndReceiveWithFixed(final String exchange, final String routingKey, final Message message) {
		return this.execute(new ChannelCallback<Message>() {

			@Override
			public Message doInRabbit(Channel channel) throws Exception {
				final PendingReply pendingReply = new PendingReply();
				String messageTag = UUID.randomUUID().toString();
				RabbitTemplate.this.replyHolder.put(messageTag, pendingReply);
				// Save any existing replyTo and correlation data
				String savedReplyTo = message.getMessageProperties().getReplyTo();
				pendingReply.setSavedReplyTo(savedReplyTo);
				if (StringUtils.hasLength(savedReplyTo) && logger.isDebugEnabled()) {
					logger.debug("Replacing replyTo header:" + savedReplyTo
							+ " in favor of template's configured reply-queue:"
							+ RabbitTemplate.this.replyAddress);
				}
				message.getMessageProperties().setReplyTo(RabbitTemplate.this.replyAddress);
				String savedCorrelation = null;
				if (RabbitTemplate.this.correlationKey == null) { // using standard correlationId property
					byte[] correlationId = message.getMessageProperties().getCorrelationId();
					if (correlationId != null) {
						savedCorrelation = new String(correlationId,
								RabbitTemplate.this.encoding);
					}
				}
				else {
					savedCorrelation = (String) message.getMessageProperties()
							.getHeaders().get(RabbitTemplate.this.correlationKey);
				}
				pendingReply.setSavedCorrelation(savedCorrelation);
				if (RabbitTemplate.this.correlationKey == null) { // using standard correlationId property
					message.getMessageProperties().setCorrelationId(messageTag
							.getBytes(RabbitTemplate.this.encoding));
				}
				else {
					message.getMessageProperties().setHeader(
							RabbitTemplate.this.correlationKey, messageTag);
				}

				if (logger.isDebugEnabled()) {
					logger.debug("Sending message with tag " + messageTag);
				}
				doSend(channel, exchange, routingKey, message, null);
				LinkedBlockingQueue<Message> replyHandoff = pendingReply.getQueue();
				Message reply = (replyTimeout < 0) ? replyHandoff.take() : replyHandoff.poll(replyTimeout,
						TimeUnit.MILLISECONDS);
				RabbitTemplate.this.replyHolder.remove(messageTag);
				return reply;
			}
		}, obtainTargetConnectionFactoryIfNecessary(this.sendConnectionFactorySelectorExpression, message));
	}

	@Override
	public <T> T execute(ChannelCallback<T> action) {
		return execute(action, null);
	}

	@SuppressWarnings("unchecked")
	private <T> T execute(final ChannelCallback<T> action, final ConnectionFactory connectionFactory) {
		if (this.retryTemplate != null) {
			try {
				return this.retryTemplate.execute(new RetryCallback<T, Exception>() {

					@Override
					public T doWithRetry(RetryContext context) throws Exception {
						return RabbitTemplate.this.doExecute(action, connectionFactory);
					}

				}, (RecoveryCallback<T>) this.recoveryCallback);
			}
			catch (Exception e) {
				if (e instanceof RuntimeException) {
					throw (RuntimeException) e;
				}
				throw RabbitExceptionTranslator.convertRabbitAccessException(e);
			}
		}
		else {
			return this.doExecute(action, connectionFactory);
		}
	}


	private <T> T doExecute(ChannelCallback<T> action, ConnectionFactory connectionFactory) {
		Assert.notNull(action, "Callback object must not be null");
		RabbitResourceHolder resourceHolder = ConnectionFactoryUtils.getTransactionalResourceHolder(
				(connectionFactory != null ? connectionFactory : getConnectionFactory()), isChannelTransacted());
		Channel channel = resourceHolder.getChannel();
		if (this.confirmCallback != null || this.returnCallback != null) {
			addListener(channel);
		}
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Executing callback on RabbitMQ Channel: " + channel);
			}
			return action.doInRabbit(channel);
		}
		catch (Exception ex) {
			if (isChannelLocallyTransacted(channel)) {
				resourceHolder.rollbackAll();
			}
			throw convertRabbitAccessException(ex);
		}
		finally {
			ConnectionFactoryUtils.releaseResources(resourceHolder);
		}
	}

	/**
	 * Send the given message to the specified exchange.
	 *
	 * @param channel The RabbitMQ Channel to operate within.
	 * @param exchange The name of the RabbitMQ exchange to send to.
	 * @param routingKey The routing key.
	 * @param message The Message to send.
	 * @param correlationData The correlation data.
	 * @throws IOException If thrown by RabbitMQ API methods
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
		boolean mandatory = this.returnCallback != null &&
				this.mandatoryExpression.getValue(this.evaluationContext, message, Boolean.class);
		MessageProperties messageProperties = message.getMessageProperties();
		if (mandatory) {
			messageProperties.getHeaders().put(PublisherCallbackChannel.RETURN_CORRELATION, this.uuid);
		}
		if (this.beforePublishPostProcessors != null) {
			for (MessagePostProcessor processor : this.beforePublishPostProcessors) {
				message = processor.postProcessMessage(message);
			}
		}
		BasicProperties convertedMessageProperties = this.messagePropertiesConverter
				.fromMessageProperties(messageProperties, encoding);
		channel.basicPublish(exchange, routingKey, mandatory, convertedMessageProperties, message.getBody());
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

	private Message buildMessageFromResponse(GetResponse response) {
		MessageProperties messageProps = this.messagePropertiesConverter.toMessageProperties(
				response.getProps(), response.getEnvelope(), this.encoding);
		messageProps.setMessageCount(response.getMessageCount());
		Message message = new Message(response.getBody(), messageProps);
		if (this.afterReceivePostProcessors != null) {
			for (MessagePostProcessor processor : this.afterReceivePostProcessors) {
				message = processor.postProcessMessage(message);
			}
		}
		return message;
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

	/**
	 * Determine a reply-to Address for the given message.
	 * <p>
	 * The default implementation first checks the Rabbit Reply-To Address of the supplied request; if that is not
	 * <code>null</code> it is returned; if it is <code>null</code>, then the configured default Exchange and
	 * routing key are used to construct a reply-to Address. If the exchange property is also <code>null</code>,
	 * then an {@link AmqpException} is thrown.
	 * @param request the original incoming Rabbit message
	 * @return the reply-to Address (never <code>null</code>)
	 * @throws AmqpException if no {@link Address} can be determined
	 * @see org.springframework.amqp.core.Message#getMessageProperties()
	 * @see org.springframework.amqp.core.MessageProperties#getReplyTo()
	 */
	private Address getReplyToAddress(Message request) throws AmqpException {
		Address replyTo = request.getMessageProperties().getReplyToAddress();
		if (replyTo == null) {
			if (this.exchange == null) {
				throw new AmqpException(
						"Cannot determine ReplyTo message property value: "
								+ "Request message does not contain reply-to property, and no default Exchange was set.");
			}
			replyTo = new Address(this.exchange, this.routingKey);
		}
		return replyTo;
	}

	private void addListener(Channel channel) {
		if (channel instanceof PublisherCallbackChannel) {
			PublisherCallbackChannel publisherCallbackChannel = (PublisherCallbackChannel) channel;
			SortedMap<Long, PendingConfirm> pendingConfirms = publisherCallbackChannel.addListener(this);
			Channel key = channel instanceof ChannelProxy ? ((ChannelProxy) channel).getTargetChannel() : channel;
			if (this.pendingConfirms.putIfAbsent(key, pendingConfirms) == null
					&& logger.isDebugEnabled()) {
				logger.debug("Added pending confirms for " + channel + " to map, size now " + this.pendingConfirms.size());
			}
		}
		else {
			throw new IllegalStateException(
					"Channel does not support confirms or returns; " +
					"is the connection factory configured for confirms or returns?");
		}
	}

	@Override
	public void handleConfirm(PendingConfirm pendingConfirm, boolean ack) {
		if (this.confirmCallback != null) {
			this.confirmCallback.confirm(pendingConfirm.getCorrelationData(), ack, pendingConfirm.getCause());
		}
		else {
			if (logger.isDebugEnabled()) {
				logger.warn("Confirm received but no callback available");
			}
		}
	}

	@Override
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

	@Override
	public boolean isConfirmListener() {
		return this.confirmCallback != null;
	}

	@Override
	public boolean isReturnListener() {
		return this.returnCallback != null;
	}

	@Override
	public void removePendingConfirmsReference(Channel channel,
			SortedMap<Long, PendingConfirm> unconfirmed) {
		this.pendingConfirms.remove(channel);
		if (logger.isDebugEnabled()) {
			logger.debug("Removed pending confirms for " + channel + " from map, size now " + this.pendingConfirms.size());
		}
	}

	@Override
	public String getUUID() {
		return this.uuid;
	}

	@Override
	public void onMessage(Message message) {
		try {
			String messageTag;
			if (this.correlationKey == null) { // using standard correlationId property
				messageTag = new String(message.getMessageProperties().getCorrelationId(), this.encoding);
			}
			else {
				messageTag = (String) message.getMessageProperties()
						.getHeaders().get(this.correlationKey);
			}
			if (messageTag == null) {
				logger.error("No correlation header in reply");
				return;
			}

			PendingReply pendingReply = this.replyHolder.get(messageTag);
			if (pendingReply == null) {
				if (logger.isWarnEnabled()) {
					logger.warn("Reply received after timeout for " + messageTag);
				}
				throw new AmqpRejectAndDontRequeueException("Reply received after timeout");
			}
			else {
				// Restore the inbound correlation data
				String savedCorrelation = pendingReply.getSavedCorrelation();
				if (this.correlationKey == null) {
					if (savedCorrelation == null) {
						message.getMessageProperties().setCorrelationId(null);
					}
					else {
						message.getMessageProperties().setCorrelationId(
								savedCorrelation.getBytes(this.encoding));
					}
				}
				else {
					if (savedCorrelation != null) {
						message.getMessageProperties().setHeader(this.correlationKey,
							savedCorrelation);
					}
					else {
						message.getMessageProperties().getHeaders().remove(this.correlationKey);
					}
				}
				// Restore any inbound replyTo
				String savedReplyTo = pendingReply.getSavedReplyTo();
				message.getMessageProperties().setReplyTo(savedReplyTo);
				LinkedBlockingQueue<Message> queue = pendingReply.getQueue();
				queue.add(message);
				if (logger.isDebugEnabled()) {
					logger.debug("Reply received for " + messageTag);
					if (savedReplyTo != null) {
						logger.debug("Restored replyTo to " + savedReplyTo);
					}
				}
			}
		}
		catch (UnsupportedEncodingException e) {
			throw new AmqpIllegalStateException("Invalid Character Set:" + this.encoding, e);
		}
	}

	private static class PendingReply {

		private volatile String savedReplyTo;

		private volatile String savedCorrelation;

		private final LinkedBlockingQueue<Message> queue;

		public PendingReply() {
			this.queue = new LinkedBlockingQueue<Message>();
		}

		public String getSavedReplyTo() {
			return savedReplyTo;
		}

		public void setSavedReplyTo(String savedReplyTo) {
			this.savedReplyTo = savedReplyTo;
		}

		public String getSavedCorrelation() {
			return savedCorrelation;
		}

		public void setSavedCorrelation(String savedCorrelation) {
			this.savedCorrelation = savedCorrelation;
		}

		public LinkedBlockingQueue<Message> getQueue() {
			return queue;
		}

	}

	public interface ConfirmCallback {

		/**
		 * Confirmation callback.
		 * @param correlationData Correlation data for the callback.
		 * @param ack true for ack, false for nack
		 * @param cause An optional cause, for nack, when available, otherwise null.
		 */
		void confirm(CorrelationData correlationData, boolean ack, String cause);

	}

	public interface ReturnCallback {
		void returnedMessage(Message message, int replyCode, String replyText,
				String exchange, String routingKey);
	}
}
