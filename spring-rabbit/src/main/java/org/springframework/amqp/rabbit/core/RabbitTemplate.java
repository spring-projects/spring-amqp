/*
 * Copyright 2002-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpMessageReturnedException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReceiveAndReplyCallback;
import org.springframework.amqp.core.ReceiveAndReplyMessageCallback;
import org.springframework.amqp.core.ReplyToAddressCallback;
import org.springframework.amqp.rabbit.connection.AbstractRoutingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.ClosingRecoveryListener;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.connection.PendingConfirm;
import org.springframework.amqp.rabbit.connection.PublisherCallbackChannel;
import org.springframework.amqp.rabbit.connection.RabbitAccessor;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;
import org.springframework.amqp.rabbit.support.ConsumerCancelledException;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.Delivery;
import org.springframework.amqp.rabbit.support.ListenerContainerAware;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.amqp.rabbit.support.ValueExpression;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.converter.SmartMessageConverter;
import org.springframework.amqp.support.postprocessor.MessagePostProcessorUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.context.expression.MapAccessor;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

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
 * @author Ernest Sadykov
 * @author Mark Norkin
 * @author Mohammad Hewedy
 * @author Alexey Platonov
 *
 * @since 1.0
 */
public class RabbitTemplate extends RabbitAccessor // NOSONAR type line count
		implements BeanFactoryAware, RabbitOperations, MessageListener,
		ListenerContainerAware, PublisherCallbackChannel.Listener, BeanNameAware, DisposableBean {

	private static final String UNCHECKED = "unchecked";

	private static final String RETURN_CORRELATION_KEY = "spring_request_return_correlation";

	/** Alias for amq.direct default exchange. */
	private static final String DEFAULT_EXCHANGE = "";

	private static final String DEFAULT_ROUTING_KEY = "";

	private static final long DEFAULT_REPLY_TIMEOUT = 5000;

	private static final long DEFAULT_CONSUME_TIMEOUT = 10000;

	private static final String DEFAULT_ENCODING = "UTF-8";

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	/*
	 * Not static as normal since we want this TL to be scoped within the template instance.
	 */
	private final ThreadLocal<Channel> dedicatedChannels = new ThreadLocal<>();

	private final AtomicInteger activeTemplateCallbacks = new AtomicInteger();

	private final ConcurrentMap<Channel, RabbitTemplate> publisherConfirmChannels =
			new ConcurrentHashMap<Channel, RabbitTemplate>();

	private final Map<String, PendingReply> replyHolder = new ConcurrentHashMap<String, PendingReply>();

	private final String uuid = UUID.randomUUID().toString();

	private final AtomicInteger messageTagProvider = new AtomicInteger();

	private final StandardEvaluationContext evaluationContext = new StandardEvaluationContext();

	private final ReplyToAddressCallback<?> defaultReplyToAddressCallback =
			(request, reply) -> getReplyToAddress(request);

	private final Map<ConnectionFactory, DirectReplyToMessageListenerContainer> directReplyToContainers =
			new HashMap<>();

	private final AtomicInteger containerInstance = new AtomicInteger();

	private String exchange = DEFAULT_EXCHANGE;

	private String routingKey = DEFAULT_ROUTING_KEY;

	// The default queue name that will be used for synchronous receives.
	private String defaultReceiveQueue;

	private long receiveTimeout = 0;

	private long replyTimeout = DEFAULT_REPLY_TIMEOUT;

	private MessageConverter messageConverter = new SimpleMessageConverter();

	private MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private String encoding = DEFAULT_ENCODING;

	private String replyAddress;

	@Nullable
	private ConfirmCallback confirmCallback;

	private ReturnCallback returnCallback;

	private Expression mandatoryExpression = new ValueExpression<Boolean>(false);

	private String correlationKey = null;

	private RetryTemplate retryTemplate;

	private RecoveryCallback<?> recoveryCallback;

	private Expression sendConnectionFactorySelectorExpression;

	private Expression receiveConnectionFactorySelectorExpression;

	private boolean useDirectReplyToContainer = true;

	private boolean useTemporaryReplyQueues;

	private Collection<MessagePostProcessor> beforePublishPostProcessors;

	private Collection<MessagePostProcessor> afterReceivePostProcessors;

	private CorrelationDataPostProcessor correlationDataPostProcessor;

	private Expression userIdExpression;

	private String beanName = "rabbitTemplate";

	private Executor taskExecutor;

	private boolean userCorrelationId;

	private boolean usePublisherConnection;

	private boolean noLocalReplyConsumer;

	private ErrorHandler replyErrorHandler;

	private volatile Boolean confirmsOrReturnsCapable;

	private volatile boolean publisherConfirms;

	private volatile boolean usingFastReplyTo;

	private volatile boolean evaluatedFastReplyTo;

	private volatile boolean isListener;

	/**
	 * Convenient constructor for use with setter injection. Don't forget to set the connection factory.
	 */
	public RabbitTemplate() {
		initDefaultStrategies(); // NOSONAR - intentionally overridable; other assertions will check

	}

	/**
	 * Create a rabbit template with default strategies and settings.
	 *
	 * @param connectionFactory the connection factory to use
	 */
	public RabbitTemplate(ConnectionFactory connectionFactory) {
		setConnectionFactory(connectionFactory);
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
	public void setExchange(@Nullable String exchange) {
		this.exchange = (exchange != null) ? exchange : DEFAULT_EXCHANGE;
	}

	/**
	 * @return the name of the default exchange used by this template.
	 *
	 * @since 1.6
	 */
	public String getExchange() {
		return this.exchange;
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
	 * @return the default routing key used by this template.
	 *
	 * @since 1.6
	 */
	public String getRoutingKey() {
		return this.routingKey;
	}

	/**
	 * The name of the default queue to receive messages from when none is specified explicitly.
	 * @param queue the default queue name to use for receive
	 * @since 2.1.2
	 */
	public void setDefaultReceiveQueue(String queue) {
		this.defaultReceiveQueue = queue;
	}

	/**
	 * Return the configured default receive queue.
	 * @return the queue or null if not configured.
	 * @since 2.2.22
	 */
	@Nullable
	public String getDefaultReceiveQueue() {
		return this.defaultReceiveQueue;
	}

	/**
	 * The encoding to use when converting between byte arrays and Strings in message properties.
	 *
	 * @param encoding the encoding to set
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	/**
	 * The encoding used when converting between byte arrays and Strings in message properties.
	 * @return the encoding.
	 */
	public String getEncoding() {
		return this.encoding;
	}

	/**
	 * An address for replies; if not provided, a temporary exclusive, auto-delete queue will
	 * be used for each reply, unless RabbitMQ supports 'amq.rabbitmq.reply-to' - see
	 * https://www.rabbitmq.com/direct-reply-to.html
	 * <p>The address can be a simple queue name (in which case the reply will be routed via the default
	 * exchange), or with the form {@code exchange/routingKey} to route the reply using an explicit
	 * exchange and routing key.
	 * @param replyAddress the replyAddress to set
	 */
	public synchronized void setReplyAddress(String replyAddress) {
		this.replyAddress = replyAddress;
		this.evaluatedFastReplyTo = false;
	}

	/**
	 * Specify the receive timeout in milliseconds when using {@code receive()} methods (for {@code sendAndReceive()}
	 * methods, refer to {@link #setReplyTimeout(long) replyTimeout}. By default, the value is zero, which
	 * means the {@code receive()} methods will return {@code null} immediately if there is no message
	 * available. Set to less than zero to wait for a message indefinitely.
	 * @param receiveTimeout the timeout.
	 * @since 1.5
	 */
	public void setReceiveTimeout(long receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
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
	 * Return the properties converter.
	 * @return the converter.
	 * @since 2.0
	 */
	protected MessagePropertiesConverter getMessagePropertiesConverter() {
		return this.messagePropertiesConverter;
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

	/**
	 * Set the mandatory flag when sending messages; only applies if a
	 * {@link #setReturnCallback(ReturnCallback) returnCallback} had been provided.
	 * @param mandatory the mandatory to set.
	 */
	public void setMandatory(boolean mandatory) {
		this.mandatoryExpression = new ValueExpression<>(mandatory);
	}

	/**
	 * @param mandatoryExpression a SpEL {@link Expression} to evaluate against each
	 * request message, if a {@link #setReturnCallback(ReturnCallback) returnCallback} has
	 * been provided. The result of the evaluation must be a {@code boolean} value.
	 * @since 1.4
	 */
	public void setMandatoryExpression(Expression mandatoryExpression) {
		Assert.notNull(mandatoryExpression, "'mandatoryExpression' must not be null");
		this.mandatoryExpression = mandatoryExpression;
	}

	/**
	 * @param mandatoryExpression a SpEL {@link Expression} to evaluate against each
	 * request message, if a {@link #setReturnCallback(ReturnCallback) returnCallback} has
	 * been provided. The result of the evaluation must be a {@code boolean} value.
	 * @since 2.0
	 */
	public void setMandatoryExpressionString(String mandatoryExpression) {
		Assert.notNull(mandatoryExpression, "'mandatoryExpression' must not be null");
		this.mandatoryExpression = PARSER.parseExpression(mandatoryExpression);
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
	 * {@link #execute(ChannelCallback)} return type.
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
	 * @see #addBeforePublishPostProcessors(MessagePostProcessor...)
	 */
	public void setBeforePublishPostProcessors(MessagePostProcessor... beforePublishPostProcessors) {
		Assert.notNull(beforePublishPostProcessors, "'beforePublishPostProcessors' cannot be null");
		Assert.noNullElements(beforePublishPostProcessors, "'beforePublishPostProcessors' cannot have null elements");
		this.beforePublishPostProcessors = MessagePostProcessorUtils.sort(Arrays.asList(beforePublishPostProcessors));
	}

	/**
	 * Add {@link MessagePostProcessor} that will be invoked immediately before invoking
	 * {@code Channel#basicPublish()}, after all other processing, except creating the
	 * {@link BasicProperties} from {@link MessageProperties}. May be used for operations
	 * such as compression. Processors are invoked in order, depending on {@code PriorityOrder},
	 * {@code Order} and finally unordered.
	 * <p>
	 * In contrast to {@link #setBeforePublishPostProcessors(MessagePostProcessor...)}, this
	 * method does not override the previously added beforePublishPostProcessors.
	 * @param beforePublishPostProcessors the post processor.
	 * @since 2.1.4
	 */
	public void addBeforePublishPostProcessors(MessagePostProcessor... beforePublishPostProcessors) {
		Assert.notNull(beforePublishPostProcessors, "'beforePublishPostProcessors' cannot be null");
		if (this.beforePublishPostProcessors == null) {
			this.beforePublishPostProcessors = new ArrayList<>();
		}
		this.beforePublishPostProcessors.addAll(Arrays.asList(beforePublishPostProcessors));
		this.beforePublishPostProcessors = MessagePostProcessorUtils.sort(this.beforePublishPostProcessors);
	}

	/**
	 * Remove the provided {@link MessagePostProcessor} from the {@link #beforePublishPostProcessors} list.
	 * @param beforePublishPostProcessor the MessagePostProcessor to remove.
	 * @return the boolean if the provided post processor has been removed.
	 * @since 2.1.4
	 * @see #addBeforePublishPostProcessors(MessagePostProcessor...)
	 */
	public boolean removeBeforePublishPostProcessor(MessagePostProcessor beforePublishPostProcessor) {
		Assert.notNull(beforePublishPostProcessor, "'beforePublishPostProcessor' cannot be null");
		if (this.beforePublishPostProcessors != null) {
			return this.beforePublishPostProcessors.remove(beforePublishPostProcessor);
		}
		return false;
	}

	/**
	 * Set a {@link MessagePostProcessor} that will be invoked immediately after a {@code Channel#basicGet()}
	 * and before any message conversion is performed.
	 * May be used for operations such as decompression. Processors are invoked in order,
	 * depending on {@code PriorityOrder}, {@code Order} and finally unordered.
	 * @param afterReceivePostProcessors the post processor.
	 * @since 1.5
	 * @see #addAfterReceivePostProcessors(MessagePostProcessor...)
	 */
	public void setAfterReceivePostProcessors(MessagePostProcessor... afterReceivePostProcessors) {
		Assert.notNull(afterReceivePostProcessors, "'afterReceivePostProcessors' cannot be null");
		Assert.noNullElements(afterReceivePostProcessors, "'afterReceivePostProcessors' cannot have null elements");
		this.afterReceivePostProcessors = MessagePostProcessorUtils.sort(Arrays.asList(afterReceivePostProcessors));
	}

	/**
	 * Return configured after receive {@link MessagePostProcessor}s or {@code null}.
	 * @return configured after receive {@link MessagePostProcessor}s or {@code null}.
	 * @since 2.1.5
	 */
	@Nullable
	public Collection<MessagePostProcessor> getAfterReceivePostProcessors() {
		return this.afterReceivePostProcessors != null
				? Collections.unmodifiableCollection(this.afterReceivePostProcessors)
				: null;
	}

	/**
	 * Add {@link MessagePostProcessor} that will be invoked immediately after a {@code Channel#basicGet()}
	 * and before any message conversion is performed.
	 * May be used for operations such as decompression. Processors are invoked in order,
	 * depending on {@code PriorityOrder}, {@code Order} and finally unordered.
	 * <p>
	 * In contrast to {@link #setAfterReceivePostProcessors(MessagePostProcessor...)}, this
	 * method does not override the previously added afterReceivePostProcessors.
	 * @param afterReceivePostProcessors the post processor.
	 * @since 2.1.4
	 */
	public void addAfterReceivePostProcessors(MessagePostProcessor... afterReceivePostProcessors) {
		Assert.notNull(afterReceivePostProcessors, "'afterReceivePostProcessors' cannot be null");
		if (this.afterReceivePostProcessors == null) {
			this.afterReceivePostProcessors = new ArrayList<>();
		}
		this.afterReceivePostProcessors.addAll(Arrays.asList(afterReceivePostProcessors));
		this.afterReceivePostProcessors = MessagePostProcessorUtils.sort(this.afterReceivePostProcessors);
	}

	/**
	 * Remove the provided {@link MessagePostProcessor} from the {@link #afterReceivePostProcessors} list.
	 * @param afterReceivePostProcessor the MessagePostProcessor to remove.
	 * @return the boolean if the provided post processor has been removed.
	 * @since 2.1.4
	 * @see #addAfterReceivePostProcessors(MessagePostProcessor...)
	 */
	public boolean removeAfterReceivePostProcessor(MessagePostProcessor afterReceivePostProcessor) {
		Assert.notNull(afterReceivePostProcessor, "'afterReceivePostProcessor' cannot be null");
		if (this.afterReceivePostProcessors != null) {
			return this.afterReceivePostProcessors.remove(afterReceivePostProcessor);
		}
		return false;
	}

	/**
	 * Set a {@link CorrelationDataPostProcessor} to be invoked before publishing a message.
	 * Correlation data is used to correlate publisher confirms.
	 * @param correlationDataPostProcessor the post processor.
	 * @since 1.6.7
	 * @see #setConfirmCallback(ConfirmCallback)
	 */
	public void setCorrelationDataPostProcessor(CorrelationDataPostProcessor correlationDataPostProcessor) {
		this.correlationDataPostProcessor = correlationDataPostProcessor;
	}

	/**
	 * By default, when the broker supports it and no
	 * {@link #setReplyAddress(String) replyAddress} is provided, send/receive
	 * methods will use Direct reply-to (https://www.rabbitmq.com/direct-reply-to.html).
	 * Setting this property to true will override that behavior and use
	 * a temporary, auto-delete, queue for each request instead.
	 * Changing this property has no effect once the first request has been
	 * processed.
	 * @param value true to use temporary queues.
	 * @since 1.6
	 */
	public void setUseTemporaryReplyQueues(boolean value) {
		this.useTemporaryReplyQueues = value;
	}

	/**
	 * Set whether or not to use a {@link DirectReplyToMessageListenerContainer} when
	 * direct reply-to is available and being used. When false, a new consumer is created
	 * for each request (the mechanism used in versions prior to 2.0). Default true.
	 * @param useDirectReplyToContainer set to false to use a consumer per request.
	 * @since 2.0
	 * @see #setUseTemporaryReplyQueues(boolean)
	 */
	public void setUseDirectReplyToContainer(boolean useDirectReplyToContainer) {
		this.useDirectReplyToContainer = useDirectReplyToContainer;
	}

	/**
	 * Set an expression to be evaluated to set the userId message property if it
	 * evaluates to a non-null value and the property is not already set in the
	 * message to be sent.
	 * See https://www.rabbitmq.com/validated-user-id.html
	 * @param userIdExpression the expression.
	 * @since 1.6
	 */
	public void setUserIdExpression(Expression userIdExpression) {
		this.userIdExpression = userIdExpression;
	}

	/**
	 * Set an expression to be evaluated to set the userId message property if it
	 * evaluates to a non-null value and the property is not already set in the
	 * message to be sent.
	 * See https://www.rabbitmq.com/validated-user-id.html
	 * @param userIdExpression the expression.
	 * @since 1.6
	 */
	public void setUserIdExpressionString(String userIdExpression) {
		this.userIdExpression = PARSER.parseExpression(userIdExpression);
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Set a task executor to use when using a {@link DirectReplyToMessageListenerContainer}.
	 * @param taskExecutor the executor.
	 * @since 2.0
	 */
	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * Set to true to use correlation id provided by the message instead of generating
	 * the correlation id for request/reply scenarios. The correlation id must be unique
	 * for all in-process requests to avoid cross talk.
	 * <p>
	 * <b>Users must therefore take create care to ensure uniqueness.</b>
	 * @param userCorrelationId true to use user correlation data.
	 */
	public void setUserCorrelationId(boolean userCorrelationId) {
		this.userCorrelationId = userCorrelationId;
	}

	/**
	 * True if separate publisher connection(s) are used.
	 * @return true or false.
	 * @since 2.0.2
	 * @see #setUsePublisherConnection(boolean)
	 */
	public boolean isUsePublisherConnection() {
		return this.usePublisherConnection;
	}

	/**
	 * To avoid deadlocked connections, it is generally recommended to use a separate
	 * connection for publishers and consumers (except when a publisher is participating
	 * in a consumer transaction). Default 'false'. When setting this to true, be careful
	 * in that a {@link RabbitAdmin} that uses this template will declare queues on the
	 * publisher connection; this may not be what you expect, especially with exclusive
	 * queues that might be consumed in this application.
	 * @param usePublisherConnection true to use a publisher connection.
	 * @since 2.0.2
	 */
	public void setUsePublisherConnection(boolean usePublisherConnection) {
		this.usePublisherConnection = usePublisherConnection;
	}

	/**
	 * Set to true for a no-local consumer. Defaults to false.
	 * @param noLocalReplyConsumer true for a no-local consumer.
	 * @since 2.1
	 * @see AbstractMessageListenerContainer#setNoLocal(boolean)
	 * @see Channel#basicConsume(String, boolean, String, boolean, boolean, Map, com.rabbitmq.client.Consumer)
	 */
	public void setNoLocalReplyConsumer(boolean noLocalReplyConsumer) {
		this.noLocalReplyConsumer = noLocalReplyConsumer;
	}

	/**
	 * When using a direct reply-to container for request/reply operations, set an error
	 * handler to be invoked when a reply delivery fails (e.g. due to a late reply).
	 * @param replyErrorHandler the reply error handler
	 * @since 2.0.11
	 * @see #setUseDirectReplyToContainer(boolean)
	 */
	public void setReplyErrorHandler(ErrorHandler replyErrorHandler) {
		this.replyErrorHandler = replyErrorHandler;
	}

	/**
	 * Invoked by the container during startup so it can verify the queue is correctly
	 * configured (if a simple reply queue name is used instead of exchange/routingKey).
	 * @return the queue name, if configured.
	 * @since 1.5
	 */
	@Override
	@Nullable
	public Collection<String> expectedQueueNames() {
		this.isListener = true;
		Collection<String> replyQueue = null;
		if (this.replyAddress == null || this.replyAddress.equals(Address.AMQ_RABBITMQ_REPLY_TO)) {
			throw new IllegalStateException("A listener container must not be provided when using direct reply-to");
		}
		else {
			Address address = new Address(this.replyAddress);
			if ("".equals(address.getExchangeName())) {
				replyQueue = Collections.singletonList(address.getRoutingKey());
			}
			else {
				if (logger.isInfoEnabled()) {
					logger.info("Cannot verify reply queue because 'replyAddress' is not a simple queue name: "
							+ this.replyAddress);
				}
			}
		}
		return replyQueue;
	}

	/**
	 * Gets unconfirmed correlation data older than age and removes them.
	 * @param age in milliseconds
	 * @return the collection of correlation data for which confirms have
	 * not been received or null if no such confirms exist.
	 */
	@Nullable
	public Collection<CorrelationData> getUnconfirmed(long age) {
		Set<CorrelationData> unconfirmed = new HashSet<>();
		long cutoffTime = System.currentTimeMillis() - age;
		for (Channel channel : this.publisherConfirmChannels.keySet()) {
			Collection<PendingConfirm> confirms = ((PublisherCallbackChannel) channel).expire(this, cutoffTime);
			for (PendingConfirm confirm : confirms) {
				unconfirmed.add(confirm.getCorrelationData());
			}
		}
		return unconfirmed.size() > 0 ? unconfirmed : null;
	}

	/**
	 * Gets unconfirmed messages count.
	 * @return The count of the messages that are not confirmed yet by RabbitMQ.
	 * @since 2.0
	 */
	public int getUnconfirmedCount() {
		return this.publisherConfirmChannels.keySet()
				.stream()
				.mapToInt(channel -> ((PublisherCallbackChannel) channel).getPendingConfirmsCount(this))
				.sum();
	}

	@Override
	public void start() {
		doStart();
	}

	/**
	 * Perform additional start actions.
	 * @since 2.0
	 */
	protected void doStart() {
		// NOSONAR
	}

	@Override
	public void stop() {
		synchronized (this.directReplyToContainers) {
			this.directReplyToContainers.values()
					.stream()
					.filter(AbstractMessageListenerContainer::isRunning)
					.forEach(AbstractMessageListenerContainer::stop);
			this.directReplyToContainers.clear();
		}
		doStop();
	}

	/**
	 * Perform additional stop actions.
	 * @since 2.0
	 */
	protected void doStop() {
		// NOSONAR
	}

	@Override
	public boolean isRunning() {
		synchronized (this.directReplyToContainers) {
			return this.directReplyToContainers.values()
					.stream()
					.anyMatch(AbstractMessageListenerContainer::isRunning);
		}
	}

	@Override
	public void destroy() {
		stop();
	}

	private void evaluateFastReplyTo() {
		this.usingFastReplyTo = useDirectReplyTo();
		this.evaluatedFastReplyTo = true;
	}

	/**
	 * Override this method use some other criteria to decide whether or not to use
	 * direct reply-to (https://www.rabbitmq.com/direct-reply-to.html).
	 * The default implementation returns true if the broker supports it and there
	 * is no {@link #setReplyAddress(String) replyAddress} set and
	 * {@link #setUseTemporaryReplyQueues(boolean) useTemporaryReplyQueues} is false.
	 * When direct reply-to is not used, the template
	 * will create a temporary, exclusive, auto-delete queue for the reply.
	 * <p>
	 * This method is invoked once only - when the first message is sent, from a
	 * synchronized block.
	 * @return true to use direct reply-to.
	 */
	protected boolean useDirectReplyTo() {
		if (this.useTemporaryReplyQueues) {
			if (this.replyAddress != null) {
				logger.warn("'useTemporaryReplyQueues' is ignored when a 'replyAddress' is provided");
			}
			else {
				return false;
			}
		}
		if (this.replyAddress == null || Address.AMQ_RABBITMQ_REPLY_TO.equals(this.replyAddress)) {
			try {
				return execute(channel -> { // NOSONAR - never null
					channel.queueDeclarePassive(Address.AMQ_RABBITMQ_REPLY_TO);
					return true;
				});
			}
			catch (AmqpConnectException | AmqpIOException ex) {
				if (shouldRethrow(ex)) {
					throw ex;
				}
			}
		}
		return false;
	}

	private boolean shouldRethrow(AmqpException ex) {
		Throwable cause = ex;
		while (cause != null && !(cause instanceof ShutdownSignalException)) {
			cause = cause.getCause();
		}
		if (cause != null && RabbitUtils.isPassiveDeclarationChannelClose((ShutdownSignalException) cause)) {
			if (logger.isWarnEnabled()) {
				logger.warn("Broker does not support fast replies via 'amq.rabbitmq.reply-to', temporary "
						+ "queues will be used: " + cause.getMessage() + ".");
			}
			this.replyAddress = null;
			return false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("IO error, deferring directReplyTo detection: " + ex.toString());
		}
		return true;
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
		send(exchange, routingKey, message, null);
	}

	@Override
	public void send(final String exchange, final String routingKey,
			final Message message, @Nullable final CorrelationData correlationData)
			throws AmqpException {
		execute(channel -> {
			doSend(channel, exchange, routingKey, message,
					(RabbitTemplate.this.returnCallback != null
							|| (correlationData != null && StringUtils.hasText(correlationData.getId())))
							&& RabbitTemplate.this.mandatoryExpression.getValue(
							RabbitTemplate.this.evaluationContext, message, Boolean.class),
					correlationData);
			return null;
		}, obtainTargetConnectionFactory(this.sendConnectionFactorySelectorExpression, message));
	}

	private ConnectionFactory obtainTargetConnectionFactory(Expression expression, Object rootObject) {
		if (expression != null && getConnectionFactory() instanceof AbstractRoutingConnectionFactory) {
			AbstractRoutingConnectionFactory routingConnectionFactory =
					(AbstractRoutingConnectionFactory) getConnectionFactory();
			Object lookupKey;
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
		return getConnectionFactory();
	}

	@Override
	public void convertAndSend(Object object) throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, object, (CorrelationData) null);
	}

	@Override
	public void correlationConvertAndSend(Object object, CorrelationData correlationData) throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, object, correlationData);
	}

	@Override
	public void convertAndSend(String routingKey, final Object object) throws AmqpException {
		convertAndSend(this.exchange, routingKey, object, (CorrelationData) null);
	}

	@Override
	public void convertAndSend(String routingKey, final Object object, CorrelationData correlationData)
			throws AmqpException {
		convertAndSend(this.exchange, routingKey, object, correlationData);
	}

	@Override
	public void convertAndSend(String exchange, String routingKey, final Object object) throws AmqpException {
		convertAndSend(exchange, routingKey, object, (CorrelationData) null);
	}

	@Override
	public void convertAndSend(String exchange, String routingKey, final Object object,
			@Nullable CorrelationData correlationData) throws AmqpException {

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

	@Override
	public void convertAndSend(Object message, MessagePostProcessor messagePostProcessor,
			CorrelationData correlationData)
			throws AmqpException {
		convertAndSend(this.exchange, this.routingKey, message, messagePostProcessor, correlationData);
	}

	@Override
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

	@Override
	public void convertAndSend(String exchange, String routingKey, final Object message,
			final MessagePostProcessor messagePostProcessor,
			@Nullable CorrelationData correlationData) throws AmqpException {
		Message messageToSend = convertMessageIfNecessary(message);
		messageToSend = messagePostProcessor.postProcessMessage(messageToSend, correlationData);
		send(exchange, routingKey, messageToSend, correlationData);
	}

	@Override
	@Nullable
	public Message receive() throws AmqpException {
		return this.receive(getRequiredQueue());
	}

	@Override
	@Nullable
	public Message receive(String queueName) {
		if (this.receiveTimeout == 0) {
			return doReceiveNoWait(queueName);
		}
		else {
			return receive(queueName, this.receiveTimeout);
		}
	}

	/**
	 * Non-blocking receive.
	 * @param queueName the queue to receive from.
	 * @return The message, or null if none immediately available.
	 * @since 1.5
	 */
	@Nullable
	protected Message doReceiveNoWait(final String queueName) {
		Message message = execute(channel -> {
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
		}, obtainTargetConnectionFactory(this.receiveConnectionFactorySelectorExpression, queueName));
		logReceived(message);
		return message;
	}

	@Override
	@Nullable
	public Message receive(long timeoutMillis) throws AmqpException {
		String queue = getRequiredQueue();
		if (timeoutMillis == 0) {
			return doReceiveNoWait(queue);
		}
		else {
			return receive(queue, timeoutMillis);
		}
	}

	@Override
	@Nullable
	public Message receive(final String queueName, final long timeoutMillis) {
		Message message = execute(channel -> {
			Delivery delivery = consumeDelivery(channel, queueName, timeoutMillis);
			if (delivery == null) {
				return null;
			}
			else {
				if (isChannelLocallyTransacted(channel)) {
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
					channel.txCommit();
				}
				else if (isChannelTransacted()) {
					ConnectionFactoryUtils.registerDeliveryTag(getConnectionFactory(), channel,
							delivery.getEnvelope().getDeliveryTag());
				}
				else {
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				}
				return buildMessageFromDelivery(delivery);
			}
		});
		logReceived(message);
		return message;
	}

	@Override
	@Nullable
	public Object receiveAndConvert() throws AmqpException {
		return receiveAndConvert(this.getRequiredQueue());
	}

	@Override
	@Nullable
	public Object receiveAndConvert(String queueName) throws AmqpException {
		return receiveAndConvert(queueName, this.receiveTimeout);
	}

	@Override
	@Nullable
	public Object receiveAndConvert(long timeoutMillis) throws AmqpException {
		return receiveAndConvert(this.getRequiredQueue(), timeoutMillis);
	}

	@Override
	@Nullable
	public Object receiveAndConvert(String queueName, long timeoutMillis) throws AmqpException {
		Message response = timeoutMillis == 0 ? doReceiveNoWait(queueName) : receive(queueName, timeoutMillis);
		if (response != null) {
			return getRequiredMessageConverter().fromMessage(response);
		}
		return null;
	}

	@Override
	@Nullable
	public <T> T receiveAndConvert(ParameterizedTypeReference<T> type) throws AmqpException {
		return receiveAndConvert(this.getRequiredQueue(), type);
	}

	@Override
	@Nullable
	public <T> T receiveAndConvert(String queueName, ParameterizedTypeReference<T> type) throws AmqpException {
		return receiveAndConvert(queueName, this.receiveTimeout, type);
	}

	@Override
	@Nullable
	public <T> T receiveAndConvert(long timeoutMillis, ParameterizedTypeReference<T> type) throws AmqpException {
		return receiveAndConvert(this.getRequiredQueue(), timeoutMillis, type);
	}

	@Override
	@SuppressWarnings(UNCHECKED)
	@Nullable
	public <T> T receiveAndConvert(String queueName, long timeoutMillis, ParameterizedTypeReference<T> type) throws AmqpException {
		Message response = timeoutMillis == 0 ? doReceiveNoWait(queueName) : receive(queueName, timeoutMillis);
		if (response != null) {
			return (T) getRequiredSmartMessageConverter().fromMessage(response, type);
		}
		return null;
	}

	@Override
	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback) throws AmqpException {
		return receiveAndReply(this.getRequiredQueue(), callback);
	}

	@Override
	@SuppressWarnings(UNCHECKED)
	public <R, S> boolean receiveAndReply(final String queueName, ReceiveAndReplyCallback<R, S> callback) throws AmqpException {
		return receiveAndReply(queueName, callback, (ReplyToAddressCallback<S>) this.defaultReplyToAddressCallback);

	}

	@Override
	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback, final String exchange,
			final String routingKey) throws AmqpException {

		return receiveAndReply(this.getRequiredQueue(), callback, exchange, routingKey);
	}

	@Override
	public <R, S> boolean receiveAndReply(final String queueName, ReceiveAndReplyCallback<R, S> callback,
			final String replyExchange, final String replyRoutingKey) throws AmqpException {

		return receiveAndReply(queueName, callback,
				(request, reply) -> new Address(replyExchange, replyRoutingKey));
	}

	@Override
	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback,
			ReplyToAddressCallback<S> replyToAddressCallback) throws AmqpException {

		return receiveAndReply(this.getRequiredQueue(), callback, replyToAddressCallback);
	}

	@Override
	public <R, S> boolean receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback,
			ReplyToAddressCallback<S> replyToAddressCallback) throws AmqpException {
		return doReceiveAndReply(queueName, callback, replyToAddressCallback);
	}

	private <R, S> boolean doReceiveAndReply(final String queueName, final ReceiveAndReplyCallback<R, S> callback,
			final ReplyToAddressCallback<S> replyToAddressCallback) throws AmqpException {

		Boolean result = execute(channel -> {
			Message receiveMessage = receiveForReply(queueName, channel);
			if (receiveMessage != null) {
				return sendReply(callback, replyToAddressCallback, channel, receiveMessage);
			}
			return false;
		}, obtainTargetConnectionFactory(this.receiveConnectionFactorySelectorExpression, queueName));
		return result == null ? false : result;
	}

	@Nullable
	private Message receiveForReply(final String queueName, Channel channel) throws IOException {
		boolean channelTransacted = isChannelTransacted();
		boolean channelLocallyTransacted = isChannelLocallyTransacted(channel);
		Message receiveMessage = null;
		if (RabbitTemplate.this.receiveTimeout == 0) {
			GetResponse response = channel.basicGet(queueName, !channelTransacted);
			// Response can be null in the case that there is no message on the queue.
			if (response != null) {
				long deliveryTag1 = response.getEnvelope().getDeliveryTag();

				if (channelLocallyTransacted) {
					channel.basicAck(deliveryTag1, false);
				}
				else if (channelTransacted) {
					// Not locally transacted but it is transacted so it could be
					// synchronized with an external transaction
					ConnectionFactoryUtils.registerDeliveryTag(getConnectionFactory(), channel, deliveryTag1);
				}
				receiveMessage = buildMessageFromResponse(response);
			}
		}
		else {
			Delivery delivery = consumeDelivery(channel, queueName, this.receiveTimeout);
			if (delivery != null) {
				long deliveryTag2 = delivery.getEnvelope().getDeliveryTag();
				if (channelTransacted && !channelLocallyTransacted) {
					// Not locally transacted but it is transacted so it could be
					// synchronized with an external transaction
					ConnectionFactoryUtils.registerDeliveryTag(getConnectionFactory(), channel, deliveryTag2);
				}
				else {
					channel.basicAck(deliveryTag2, false);
				}
				receiveMessage = buildMessageFromDelivery(delivery);
			}
		}
		logReceived(receiveMessage);
		return receiveMessage;
	}

	@Nullable // NOSONAR complexity
	private Delivery consumeDelivery(Channel channel, String queueName, long timeoutMillis)
			throws IOException {

		Delivery delivery = null;
		RuntimeException exception = null;
		CompletableFuture<Delivery> future = new CompletableFuture<>();
		ShutdownListener shutdownListener = c -> {
			if (!RabbitUtils.isNormalChannelClose(c)) {
				future.completeExceptionally(c);
			}
		};
		channel.addShutdownListener(shutdownListener);
		ClosingRecoveryListener.addRecoveryListenerIfNecessary(channel);
		DefaultConsumer consumer = null;
		try {
			consumer = createConsumer(queueName, channel, future,
					timeoutMillis < 0 ? DEFAULT_CONSUME_TIMEOUT : timeoutMillis);
			if (timeoutMillis < 0) {
				delivery = future.get();
			}
			else {
				delivery = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
			}
		}
		catch (ExecutionException e) {
			Throwable cause = e.getCause();
			this.logger.error("Consumer failed to receive message: " + consumer, cause);
			exception = RabbitExceptionTranslator.convertRabbitAccessException(cause);
			throw exception;
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		catch (TimeoutException e) {
			RabbitUtils.setPhysicalCloseRequired(channel, true);
		}
		finally {
			if (consumer != null && !(exception instanceof ConsumerCancelledException) && channel.isOpen()) {
				cancelConsumerQuietly(channel, consumer);
			}
			try {
				channel.removeShutdownListener(shutdownListener);
			}
			catch (Exception e) {
				// NOSONAR - channel might have closed.
			}
		}
		return delivery;
	}

	private void logReceived(Message message) {
		if (message == null) {
			logger.debug("Received no message");
		}
		else if (logger.isDebugEnabled()) {
			logger.debug("Received: " + message);
		}
	}

	@SuppressWarnings(UNCHECKED)
	private <R, S> boolean sendReply(final ReceiveAndReplyCallback<R, S> callback,
			final ReplyToAddressCallback<S> replyToAddressCallback, Channel channel, Message receiveMessage)
			throws IOException {

		Object receive = receiveMessage;
		if (!(ReceiveAndReplyMessageCallback.class.isAssignableFrom(callback.getClass()))) {
			receive = getRequiredMessageConverter().fromMessage(receiveMessage);
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
			doSendReply(replyToAddressCallback, channel, receiveMessage, reply);
		}
		else if (isChannelLocallyTransacted(channel)) {
			channel.txCommit();
		}

		return true;
	}

	private <S> void doSendReply(final ReplyToAddressCallback<S> replyToAddressCallback, Channel channel,
			Message receiveMessage, S reply) throws IOException {

		Address replyTo = replyToAddressCallback.getReplyToAddress(receiveMessage, reply);

		Message replyMessage = convertMessageIfNecessary(reply);

		MessageProperties receiveMessageProperties = receiveMessage.getMessageProperties();
		MessageProperties replyMessageProperties = replyMessage.getMessageProperties();

		Object correlation = this.correlationKey == null
				? receiveMessageProperties.getCorrelationId()
				: receiveMessageProperties.getHeaders().get(this.correlationKey);

		if (this.correlationKey == null || correlation == null) {
			// using standard correlationId property
			if (correlation == null) {
				String messageId = receiveMessageProperties.getMessageId();
				if (messageId != null) {
					correlation = messageId;
				}
			}
			replyMessageProperties.setCorrelationId((String) correlation);
		}
		else {
			replyMessageProperties.setHeader(this.correlationKey, correlation);
		}

		// 'doSend()' takes care of 'channel.txCommit()'.
		doSend(channel,
				replyTo.getExchangeName(),
				replyTo.getRoutingKey(),
				replyMessage,
				RabbitTemplate.this.returnCallback != null && isMandatoryFor(replyMessage),
				null);
	}

	@Override
	@Nullable
	public Message sendAndReceive(final Message message) throws AmqpException {
		return sendAndReceive(message, null);
	}

	@Nullable
	public Message sendAndReceive(final Message message, @Nullable CorrelationData correlationData)
			throws AmqpException {

		return doSendAndReceive(this.exchange, this.routingKey, message, correlationData);
	}

	@Override
	@Nullable
	public Message sendAndReceive(final String routingKey, final Message message) throws AmqpException {
		return sendAndReceive(routingKey, message, null);
	}

	@Nullable
	public Message sendAndReceive(final String routingKey, final Message message,
			@Nullable CorrelationData correlationData) throws AmqpException {

		return doSendAndReceive(this.exchange, routingKey, message, correlationData);
	}

	@Override
	@Nullable
	public Message sendAndReceive(final String exchange, final String routingKey, final Message message)
			throws AmqpException {

		return sendAndReceive(exchange, routingKey, message, null);
	}

	@Nullable
	public Message sendAndReceive(final String exchange, final String routingKey, final Message message,
			@Nullable CorrelationData correlationData) throws AmqpException {

		return doSendAndReceive(exchange, routingKey, message, correlationData);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final Object message) throws AmqpException {
		return convertSendAndReceive(message, (CorrelationData) null);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final Object message, @Nullable CorrelationData correlationData)
			throws AmqpException {

		return convertSendAndReceive(this.exchange, this.routingKey, message, null, correlationData);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final String routingKey, final Object message) throws AmqpException {
		return convertSendAndReceive(routingKey, message, (CorrelationData) null);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final String routingKey, final Object message,
			@Nullable CorrelationData correlationData) throws AmqpException {

		return convertSendAndReceive(this.exchange, routingKey, message, null, correlationData);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final String exchange, final String routingKey, final Object message)
			throws AmqpException {

		return convertSendAndReceive(exchange, routingKey, message, (CorrelationData) null);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final String exchange, final String routingKey, final Object message,
			@Nullable CorrelationData correlationData) throws AmqpException {

		return convertSendAndReceive(exchange, routingKey, message, null, correlationData);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final Object message, final MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		return convertSendAndReceive(message, messagePostProcessor, null);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final Object message, final MessagePostProcessor messagePostProcessor,
			@Nullable CorrelationData correlationData) throws AmqpException {

		return convertSendAndReceive(this.exchange, this.routingKey, message, messagePostProcessor, correlationData);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final String routingKey, final Object message,
			final MessagePostProcessor messagePostProcessor) throws AmqpException {

		return convertSendAndReceive(routingKey, message, messagePostProcessor, null);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final String routingKey, final Object message,
			final MessagePostProcessor messagePostProcessor, @Nullable CorrelationData correlationData)
			throws AmqpException {

		return convertSendAndReceive(this.exchange, routingKey, message, messagePostProcessor, correlationData);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final String exchange, final String routingKey, final Object message,
			final MessagePostProcessor messagePostProcessor) throws AmqpException {
		return convertSendAndReceive(exchange, routingKey, message, messagePostProcessor, null);
	}

	@Override
	@Nullable
	public Object convertSendAndReceive(final String exchange, final String routingKey, final Object message,
			@Nullable final MessagePostProcessor messagePostProcessor,
			@Nullable final CorrelationData correlationData) throws AmqpException {

		Message replyMessage = convertSendAndReceiveRaw(exchange, routingKey, message, messagePostProcessor,
				correlationData);
		if (replyMessage == null) {
			return null;
		}
		return this.getRequiredMessageConverter().fromMessage(replyMessage);
	}

	@Override
	@Nullable
	public <T> T convertSendAndReceiveAsType(final Object message, ParameterizedTypeReference<T> responseType)
			throws AmqpException {
		return convertSendAndReceiveAsType(message, (CorrelationData) null, responseType);
	}

	@Override
	@Nullable
	public <T> T convertSendAndReceiveAsType(final Object message, @Nullable CorrelationData correlationData,
			ParameterizedTypeReference<T> responseType) throws AmqpException {

		return convertSendAndReceiveAsType(this.exchange, this.routingKey, message, null, correlationData,
				responseType);
	}

	@Override
	@Nullable
	public <T> T convertSendAndReceiveAsType(final String routingKey, final Object message,
			ParameterizedTypeReference<T> responseType) throws AmqpException {

		return convertSendAndReceiveAsType(routingKey, message, (CorrelationData) null, responseType);
	}

	@Override
	@Nullable
	public <T> T convertSendAndReceiveAsType(final String routingKey, final Object message,
			@Nullable CorrelationData correlationData, ParameterizedTypeReference<T> responseType)
			throws AmqpException {

		return convertSendAndReceiveAsType(this.exchange, routingKey, message, null, correlationData, responseType);
	}

	@Override
	@Nullable
	public <T> T convertSendAndReceiveAsType(final String exchange, final String routingKey, final Object message,
			ParameterizedTypeReference<T> responseType) throws AmqpException {

		return convertSendAndReceiveAsType(exchange, routingKey, message, (CorrelationData) null, responseType);
	}

	@Override
	@Nullable
	public <T> T convertSendAndReceiveAsType(final Object message,
			@Nullable final MessagePostProcessor messagePostProcessor,
			ParameterizedTypeReference<T> responseType) throws AmqpException {

		return convertSendAndReceiveAsType(message, messagePostProcessor, null, responseType);
	}

	@Override
	@Nullable
	public <T> T convertSendAndReceiveAsType(final Object message,
			@Nullable final MessagePostProcessor messagePostProcessor,
			@Nullable CorrelationData correlationData, ParameterizedTypeReference<T> responseType)
			throws AmqpException {

		return convertSendAndReceiveAsType(this.exchange, this.routingKey, message, messagePostProcessor,
				correlationData, responseType);
	}

	@Override
	@Nullable
	public <T> T convertSendAndReceiveAsType(final String routingKey, final Object message,
			@Nullable final MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<T> responseType)
			throws AmqpException {

		return convertSendAndReceiveAsType(routingKey, message, messagePostProcessor, null, responseType);
	}

	@Override
	@Nullable
	public <T> T convertSendAndReceiveAsType(final String routingKey, final Object message,
			@Nullable final MessagePostProcessor messagePostProcessor, @Nullable CorrelationData correlationData,
			ParameterizedTypeReference<T> responseType) throws AmqpException {

		return convertSendAndReceiveAsType(this.exchange, routingKey, message, messagePostProcessor, correlationData,
				responseType);
	}

	@Override
	@Nullable
	public <T> T convertSendAndReceiveAsType(final String exchange, final String routingKey, final Object message,
			final MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<T> responseType)
			throws AmqpException {

		return convertSendAndReceiveAsType(exchange, routingKey, message, messagePostProcessor, null, responseType);
	}

	@Override
	@SuppressWarnings(UNCHECKED)
	@Nullable
	public <T> T convertSendAndReceiveAsType(final String exchange, final String routingKey, final Object message,
			@Nullable final MessagePostProcessor messagePostProcessor, @Nullable final CorrelationData correlationData,
			ParameterizedTypeReference<T> responseType) throws AmqpException {

		Message replyMessage = convertSendAndReceiveRaw(exchange, routingKey, message, messagePostProcessor,
				correlationData);
		if (replyMessage == null) {
			return null;
		}
		return (T) getRequiredSmartMessageConverter().fromMessage(replyMessage, responseType);
	}

	/**
	 * Convert and send a message and return the raw reply message, or null. Subclasses can
	 * invoke this method if they want to perform conversion on the outbound message but
	 * have direct access to the reply message before conversion.
	 * @param exchange the exchange.
	 * @param routingKey the routing key.
	 * @param message the data to send.
	 * @param messagePostProcessor a message post processor (can be null).
	 * @param correlationData correlation data (can be null).
	 * @return the reply message or null if a timeout occurs.
	 * @since 1.6.6
	 */
	@Nullable
	protected Message convertSendAndReceiveRaw(final String exchange, final String routingKey, final Object message,
			@Nullable final MessagePostProcessor messagePostProcessor,
			@Nullable final CorrelationData correlationData) {

		Message requestMessage = convertMessageIfNecessary(message);
		if (messagePostProcessor != null) {
			requestMessage = messagePostProcessor.postProcessMessage(requestMessage, correlationData);
		}
		return doSendAndReceive(exchange, routingKey, requestMessage, correlationData);
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
	 * @param correlationData the correlation data for confirms
	 * @return the message that is received in reply
	 */
	@Nullable
	protected Message doSendAndReceive(final String exchange, final String routingKey, final Message message,
			@Nullable CorrelationData correlationData) {

		if (!this.evaluatedFastReplyTo) {
			synchronized (this) {
				if (!this.evaluatedFastReplyTo) {
					evaluateFastReplyTo();
				}
			}
		}

		if (this.usingFastReplyTo && this.useDirectReplyToContainer) {
			return doSendAndReceiveWithDirect(exchange, routingKey, message, correlationData);
		}
		else if (this.replyAddress == null || this.usingFastReplyTo) {
			return doSendAndReceiveWithTemporary(exchange, routingKey, message, correlationData);
		}
		else {
			return doSendAndReceiveWithFixed(exchange, routingKey, message, correlationData);
		}
	}

	@Nullable
	protected Message doSendAndReceiveWithTemporary(final String exchange, final String routingKey,
			final Message message, final CorrelationData correlationData) {

		return execute(channel -> {
			final PendingReply pendingReply = new PendingReply();
			String messageTag = String.valueOf(RabbitTemplate.this.messageTagProvider.incrementAndGet());
			RabbitTemplate.this.replyHolder.putIfAbsent(messageTag, pendingReply);

			Assert.isNull(message.getMessageProperties().getReplyTo(),
					"Send-and-receive methods can only be used " +
							"if the Message does not already have a replyTo property.");
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
			DefaultConsumer consumer = new TemplateConsumer(channel) {

				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
						byte[] body) {
					MessageProperties messageProperties = RabbitTemplate.this.messagePropertiesConverter
							.toMessageProperties(properties, envelope, RabbitTemplate.this.encoding);
					Message reply = new Message(body, messageProperties);
					if (logger.isTraceEnabled()) {
						logger.trace("Message received " + reply);
					}
					if (RabbitTemplate.this.afterReceivePostProcessors != null) {
						for (MessagePostProcessor processor : RabbitTemplate.this.afterReceivePostProcessors) {
							reply = processor.postProcessMessage(reply);
						}
					}
					pendingReply.reply(reply);
				}

			};
			ClosingRecoveryListener.addRecoveryListenerIfNecessary(channel);
			ShutdownListener shutdownListener = c -> {
				if (!RabbitUtils.isNormalChannelClose(c)) {
					pendingReply.completeExceptionally(c);
				}
			};
			channel.addShutdownListener(shutdownListener);
			channel.basicConsume(replyTo, true, consumerTag, this.noLocalReplyConsumer, true, null, consumer);
			Message reply = null;
			try {
				reply = exchangeMessages(exchange, routingKey, message, correlationData, channel, pendingReply,
						messageTag);
			}
			finally {
				this.replyHolder.remove(messageTag);
				if (channel.isOpen()) {
					cancelConsumerQuietly(channel, consumer);
				}
				try {
					channel.removeShutdownListener(shutdownListener);
				}
				catch (Exception e) {
					// NOSONAR - channel might have closed.
				}
			}
			return reply;
		}, obtainTargetConnectionFactory(this.sendConnectionFactorySelectorExpression, message));
	}

	private void cancelConsumerQuietly(Channel channel, DefaultConsumer consumer) {
		RabbitUtils.cancel(channel, consumer.getConsumerTag());
	}

	@Nullable
	protected Message doSendAndReceiveWithFixed(final String exchange, final String routingKey, final Message message,
			final CorrelationData correlationData) {
		Assert.state(this.isListener, () -> "RabbitTemplate is not configured as MessageListener - "
				+ "cannot use a 'replyAddress': " + this.replyAddress);
		return execute(channel -> {
			return doSendAndReceiveAsListener(exchange, routingKey, message, correlationData, channel);
		}, obtainTargetConnectionFactory(this.sendConnectionFactorySelectorExpression, message));
	}

	@Nullable
	private Message doSendAndReceiveWithDirect(String exchange, String routingKey, Message message,
			CorrelationData correlationData) {
		ConnectionFactory connectionFactory = obtainTargetConnectionFactory(
				this.sendConnectionFactorySelectorExpression, message);
		if (this.usePublisherConnection && connectionFactory.getPublisherConnectionFactory() != null) {
			connectionFactory = connectionFactory.getPublisherConnectionFactory();
		}
		DirectReplyToMessageListenerContainer container = this.directReplyToContainers.get(connectionFactory);
		if (container == null) {
			synchronized (this.directReplyToContainers) {
				container = this.directReplyToContainers.get(connectionFactory);
				if (container == null) {
					container = new DirectReplyToMessageListenerContainer(connectionFactory);
					container.setMessageListener(this);
					container.setBeanName(this.beanName + "#" + this.containerInstance.getAndIncrement());
					if (this.taskExecutor != null) {
						container.setTaskExecutor(this.taskExecutor);
					}
					container.setNoLocal(this.noLocalReplyConsumer);
					if (this.replyErrorHandler != null) {
						container.setErrorHandler(this.replyErrorHandler);
					}
					container.start();
					this.directReplyToContainers.put(connectionFactory, container);
					this.replyAddress = Address.AMQ_RABBITMQ_REPLY_TO;
				}
			}
		}
		ChannelHolder channelHolder = container.getChannelHolder();
		try {
			Channel channel = channelHolder.getChannel();
			if (this.confirmsOrReturnsCapable) {
				addListener(channel);
			}
			return doSendAndReceiveAsListener(exchange, routingKey, message, correlationData, channel);
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
		finally {
			container.releaseConsumerFor(channelHolder, false, null);
		}
	}

	@Nullable
	private Message doSendAndReceiveAsListener(final String exchange, final String routingKey, final Message message,
			final CorrelationData correlationData, Channel channel) throws Exception { // NOSONAR
		final PendingReply pendingReply = new PendingReply();
		String messageTag = null;
		if (this.userCorrelationId) {
			if (this.correlationKey != null) {
				messageTag = (String) message.getMessageProperties().getHeaders().get(this.correlationKey);
			}
			else {
				messageTag = message.getMessageProperties().getCorrelationId();
			}
		}
		if (messageTag == null) {
			messageTag = String.valueOf(this.messageTagProvider.incrementAndGet());
		}
		this.replyHolder.put(messageTag, pendingReply);
		saveAndSetProperties(message, pendingReply, messageTag);

		if (logger.isDebugEnabled()) {
			logger.debug("Sending message with tag " + messageTag);
		}
		Message reply = null;
		try {
			reply = exchangeMessages(exchange, routingKey, message, correlationData, channel, pendingReply,
					messageTag);
			if (reply != null && this.afterReceivePostProcessors != null) {
				for (MessagePostProcessor processor : this.afterReceivePostProcessors) {
					reply = processor.postProcessMessage(reply);
				}
			}
		}
		finally {
			this.replyHolder.remove(messageTag);
		}
		return reply;
	}

	private void saveAndSetProperties(final Message message, final PendingReply pendingReply, String messageTag) {
		// Save any existing replyTo and correlation data
		String savedReplyTo = message.getMessageProperties().getReplyTo();
		pendingReply.setSavedReplyTo(savedReplyTo);
		if (StringUtils.hasLength(savedReplyTo) && logger.isDebugEnabled()) {
			logger.debug("Replacing replyTo header: " + savedReplyTo
					+ " in favor of template's configured reply-queue: "
					+ RabbitTemplate.this.replyAddress);
		}
		message.getMessageProperties().setReplyTo(this.replyAddress);
		if (!this.userCorrelationId) {
			String savedCorrelation = null;
			if (this.correlationKey == null) { // using standard correlationId property
				String correlationId = message.getMessageProperties().getCorrelationId();
				if (correlationId != null) {
					savedCorrelation = correlationId;
				}
			}
			else {
				savedCorrelation = (String) message.getMessageProperties()
						.getHeaders().get(this.correlationKey);
			}
			pendingReply.setSavedCorrelation(savedCorrelation);
			if (this.correlationKey == null) { // using standard correlationId property
				message.getMessageProperties().setCorrelationId(messageTag);
			}
			else {
				message.getMessageProperties().setHeader(this.correlationKey, messageTag);
			}
		}
	}

	@Nullable
	private Message exchangeMessages(final String exchange, final String routingKey, final Message message,
			final CorrelationData correlationData, Channel channel, final PendingReply pendingReply, String messageTag)
			throws IOException, InterruptedException {

		Message reply;
		boolean mandatory = isMandatoryFor(message);
		if (mandatory && this.returnCallback == null) {
			message.getMessageProperties().getHeaders().put(RETURN_CORRELATION_KEY, messageTag);
		}
		doSend(channel, exchange, routingKey, message, mandatory, correlationData);
		reply = this.replyTimeout < 0 ? pendingReply.get() : pendingReply.get(this.replyTimeout, TimeUnit.MILLISECONDS);
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Reply: " + reply);
		}
		if (reply == null) {
			replyTimedOut(message.getMessageProperties().getCorrelationId());
		}
		return reply;
	}

	/**
	 * Subclasses can implement this to be notified that a reply has timed out.
	 * @param correlationId the correlationId
	 * @since 2.1.2
	 */
	protected void replyTimedOut(String correlationId) {
		// NOSONAR
	}

	/**
	 * Return whether the provided message should be sent with the mandatory flag set.
	 * @param message the message.
	 * @return true for mandatory.
	 * @since 2.0
	 */
	public Boolean isMandatoryFor(final Message message) {
		Boolean value = this.mandatoryExpression.getValue(this.evaluationContext, message, Boolean.class);
		return value != null ? value : Boolean.FALSE;
	}

	@Override
	@Nullable
	public <T> T execute(ChannelCallback<T> action) {
		return execute(action, getConnectionFactory());
	}

	@SuppressWarnings(UNCHECKED)
	@Nullable
	private <T> T execute(final ChannelCallback<T> action, final ConnectionFactory connectionFactory) {
		if (this.retryTemplate != null) {
			try {
				return this.retryTemplate.execute(
						(RetryCallback<T, Exception>) context -> doExecute(action, connectionFactory),
						(RecoveryCallback<T>) this.recoveryCallback);
			}
			catch (RuntimeException e) { // NOSONAR catch and rethrow needed to avoid next catch
				throw e;
			}
			catch (Exception e) {
				throw RabbitExceptionTranslator.convertRabbitAccessException(e);
			}
		}
		else {
			return doExecute(action, connectionFactory);
		}
	}

	@Nullable
	private <T> T doExecute(ChannelCallback<T> action, ConnectionFactory connectionFactory) { // NOSONAR complexity
		Assert.notNull(action, "Callback object must not be null");
		Channel channel = null;
		boolean invokeScope = false;
		// No need to check the thread local if we know that no invokes are in process
		if (this.activeTemplateCallbacks.get() > 0) {
			channel = this.dedicatedChannels.get();
		}
		RabbitResourceHolder resourceHolder = null;
		Connection connection = null; // NOSONAR (close)
		if (channel == null) {
			if (isChannelTransacted()) {
				resourceHolder =
						ConnectionFactoryUtils.getTransactionalResourceHolder(connectionFactory,
								true, this.usePublisherConnection);
				channel = resourceHolder.getChannel();
				if (channel == null) {
					ConnectionFactoryUtils.releaseResources(resourceHolder);
					throw new IllegalStateException("Resource holder returned a null channel");
				}
			}
			else {
				connection = ConnectionFactoryUtils.createConnection(connectionFactory,
						this.usePublisherConnection); // NOSONAR - RabbitUtils closes
				if (connection == null) {
					throw new IllegalStateException("Connection factory returned a null connection");
				}
				try {
					channel = connection.createChannel(false);
					if (channel == null) {
						throw new IllegalStateException("Connection returned a null channel");
					}
				}
				catch (RuntimeException e) {
					RabbitUtils.closeConnection(connection);
					throw e;
				}
			}
		}
		else {
			invokeScope = true;
		}
		try {
			return invokeAction(action, connectionFactory, channel);
		}
		catch (Exception ex) {
			if (isChannelLocallyTransacted(channel)) {
				resourceHolder.rollbackAll();
			}
			throw convertRabbitAccessException(ex);
		}
		finally {
			cleanUpAfterAction(channel, invokeScope, resourceHolder, connection);
		}
	}

	private void cleanUpAfterAction(Channel channel, boolean invokeScope, RabbitResourceHolder resourceHolder,
			Connection connection) {

		if (!invokeScope) {
			if (resourceHolder != null) {
				ConnectionFactoryUtils.releaseResources(resourceHolder);
			}
			else {
				RabbitUtils.closeChannel(channel);
				RabbitUtils.closeConnection(connection);
			}
		}
	}

	@Nullable
	private <T> T invokeAction(ChannelCallback<T> action, ConnectionFactory connectionFactory, Channel channel)
			throws Exception { // NOSONAR see the callback

		if (this.confirmsOrReturnsCapable == null) {
			determineConfirmsReturnsCapability(connectionFactory);
		}
		if (this.confirmsOrReturnsCapable) {
			addListener(channel);
		}
		if (logger.isDebugEnabled()) {
			logger.debug(
					"Executing callback " + action.getClass().getSimpleName() + " on RabbitMQ Channel: " + channel);
		}
		return action.doInRabbit(channel);
	}

	@Override
	@Nullable
	public <T> T invoke(OperationsCallback<T> action, @Nullable com.rabbitmq.client.ConfirmCallback acks,
			@Nullable com.rabbitmq.client.ConfirmCallback nacks) {

		final Channel currentChannel = this.dedicatedChannels.get();
		Assert.state(currentChannel == null, () -> "Nested invoke() calls are not supported; channel '" + currentChannel
				+ "' is already associated with this thread");
		this.activeTemplateCallbacks.incrementAndGet();
		RabbitResourceHolder resourceHolder = null;
		Connection connection = null; // NOSONAR (close)
		Channel channel;
		ConnectionFactory connectionFactory = getConnectionFactory();
		if (isChannelTransacted()) {
			resourceHolder = ConnectionFactoryUtils.getTransactionalResourceHolder(connectionFactory, true,
					this.usePublisherConnection);
			channel = resourceHolder.getChannel();
			if (channel == null) {
				ConnectionFactoryUtils.releaseResources(resourceHolder);
				throw new IllegalStateException("Resource holder returned a null channel");
			}
		}
		else {
			if (this.usePublisherConnection && connectionFactory.getPublisherConnectionFactory() != null) {
				connectionFactory = connectionFactory.getPublisherConnectionFactory();
			}
			connection = connectionFactory.createConnection(); // NOSONAR - RabbitUtils
			if (connection == null) {
				throw new IllegalStateException("Connection factory returned a null connection");
			}
			try {
				channel = connection.createChannel(false);
				if (channel == null) {
					throw new IllegalStateException("Connection returned a null channel");
				}
				if (!connectionFactory.isPublisherConfirms()) {
					RabbitUtils.setPhysicalCloseRequired(channel, true);
				}
				this.dedicatedChannels.set(channel);
			}
			catch (RuntimeException e) {
				RabbitUtils.closeConnection(connection);
				throw e;
			}
		}
		ConfirmListener listener = addConfirmListener(acks, nacks, channel);
		try {
			return action.doInRabbit(this);
		}
		finally {
			cleanUpAfterAction(resourceHolder, connection, channel, listener);
		}
	}

	@Nullable
	private ConfirmListener addConfirmListener(@Nullable com.rabbitmq.client.ConfirmCallback acks,
			@Nullable com.rabbitmq.client.ConfirmCallback nacks, Channel channel) {
		ConfirmListener listener = null;
		if (acks != null && nacks != null && channel instanceof ChannelProxy
				&& ((ChannelProxy) channel).isConfirmSelected()) {
			listener = channel.addConfirmListener(acks, nacks);
		}
		return listener;
	}

	private void cleanUpAfterAction(RabbitResourceHolder resourceHolder, Connection connection, Channel channel,
			ConfirmListener listener) {

		if (listener != null) {
			channel.removeConfirmListener(listener);
		}
		this.activeTemplateCallbacks.decrementAndGet();
		this.dedicatedChannels.remove();
		if (resourceHolder != null) {
			ConnectionFactoryUtils.releaseResources(resourceHolder);
		}
		else {
			RabbitUtils.closeChannel(channel);
			RabbitUtils.closeConnection(connection);
		}
	}

	@Override
	public boolean waitForConfirms(long timeout) {
		Channel channel = this.dedicatedChannels.get();
		Assert.state(channel != null, "This operation is only available within the scope of an invoke operation");
		try {
			return channel.waitForConfirms(timeout);
		}
		catch (TimeoutException e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	@Override
	public void waitForConfirmsOrDie(long timeout) {
		Channel channel = this.dedicatedChannels.get();
		Assert.state(channel != null, "This operation is only available within the scope of an invoke operation");
		try {
			channel.waitForConfirmsOrDie(timeout);
		}
		catch (IOException | TimeoutException e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	public void determineConfirmsReturnsCapability(ConnectionFactory connectionFactory) {
		this.publisherConfirms = connectionFactory.isPublisherConfirms();
		this.confirmsOrReturnsCapable =
				this.publisherConfirms || connectionFactory.isPublisherReturns();
	}

	/**
	 * Send the given message to the specified exchange.
	 *
	 * @param channel The RabbitMQ Channel to operate within.
	 * @param exchangeArg The name of the RabbitMQ exchange to send to.
	 * @param routingKeyArg The routing key.
	 * @param message The Message to send.
	 * @param mandatory The mandatory flag.
	 * @param correlationData The correlation data.
	 * @throws IOException If thrown by RabbitMQ API methods.
	 */
	public void doSend(Channel channel, String exchangeArg, String routingKeyArg, Message message, // NOSONAR complexity
			boolean mandatory, @Nullable CorrelationData correlationData) throws IOException {

		String exch = exchangeArg;
		String rKey = routingKeyArg;
		if (exch == null) {
			exch = this.exchange;
		}
		if (rKey == null) {
			rKey = this.routingKey;
		}

		if (logger.isTraceEnabled()) {
			logger.trace("Original message to publish: " + message);
		}

		Message messageToUse = message;
		MessageProperties messageProperties = messageToUse.getMessageProperties();
		if (mandatory) {
			messageProperties.getHeaders().put(PublisherCallbackChannel.RETURN_LISTENER_CORRELATION_KEY, this.uuid);
		}
		if (this.beforePublishPostProcessors != null) {
			for (MessagePostProcessor processor : this.beforePublishPostProcessors) {
				messageToUse = processor.postProcessMessage(messageToUse, correlationData);
			}
		}
		setupConfirm(channel, messageToUse, correlationData);
		if (this.userIdExpression != null && messageProperties.getUserId() == null) {
			String userId = this.userIdExpression.getValue(this.evaluationContext, messageToUse, String.class);
			if (userId != null) {
				messageProperties.setUserId(userId);
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Publishing message [" + messageToUse
					+ "] on exchange [" + exch + "], routingKey = [" + rKey + "]");
		}
		sendToRabbit(channel, exch, rKey, mandatory, messageToUse);
		// Check if commit needed
		if (isChannelLocallyTransacted(channel)) {
			// Transacted channel created by this template -> commit.
			RabbitUtils.commitIfNecessary(channel);
		}
	}

	protected void sendToRabbit(Channel channel, String exchange, String routingKey, boolean mandatory,
			Message message) throws IOException {
		BasicProperties convertedMessageProperties = this.messagePropertiesConverter
				.fromMessageProperties(message.getMessageProperties(), this.encoding);
		channel.basicPublish(exchange, routingKey, mandatory, convertedMessageProperties, message.getBody());
	}

	private void setupConfirm(Channel channel, Message message, @Nullable CorrelationData correlationDataArg) {
		if ((this.publisherConfirms || this.confirmCallback != null) && channel instanceof PublisherCallbackChannel) {

			PublisherCallbackChannel publisherCallbackChannel = (PublisherCallbackChannel) channel;
			CorrelationData correlationData = this.correlationDataPostProcessor != null
					? this.correlationDataPostProcessor.postProcess(message, correlationDataArg)
					: correlationDataArg;
			long nextPublishSeqNo = channel.getNextPublishSeqNo();
			message.getMessageProperties().setPublishSequenceNumber(nextPublishSeqNo);
			publisherCallbackChannel.addPendingConfirm(this, nextPublishSeqNo,
					new PendingConfirm(correlationData, System.currentTimeMillis()));
			if (correlationData != null && StringUtils.hasText(correlationData.getId())) {
				message.getMessageProperties().setHeader(PublisherCallbackChannel.RETURNED_MESSAGE_CORRELATION_KEY,
						correlationData.getId());
			}
		}
		else if (channel instanceof ChannelProxy && ((ChannelProxy) channel).isConfirmSelected()) {
			long nextPublishSeqNo = channel.getNextPublishSeqNo();
			message.getMessageProperties().setPublishSequenceNumber(nextPublishSeqNo);
		}
	}

	/**
	 * Check whether the given Channel is locally transacted, that is, whether its transaction is managed by this
	 * template's Channel handling and not by an external transaction coordinator.
	 * @param channel the Channel to check
	 * @return whether the given Channel is locally transacted
	 * @see ConnectionFactoryUtils#isChannelTransactional
	 * @see #isChannelTransacted
	 */
	protected boolean isChannelLocallyTransacted(Channel channel) {
		return isChannelTransacted() && !ConnectionFactoryUtils.isChannelTransactional(channel, getConnectionFactory());
	}

	private Message buildMessageFromDelivery(Delivery delivery) {
		return buildMessage(delivery.getEnvelope(), delivery.getProperties(), delivery.getBody(), -1);
	}

	private Message buildMessageFromResponse(GetResponse response) {
		return buildMessage(response.getEnvelope(), response.getProps(), response.getBody(),
				response.getMessageCount());
	}

	private Message buildMessage(Envelope envelope, BasicProperties properties, byte[] body, int msgCount) {
		MessageProperties messageProps = this.messagePropertiesConverter.toMessageProperties(
				properties, envelope, this.encoding);
		if (msgCount >= 0) {
			messageProps.setMessageCount(msgCount);
		}
		Message message = new Message(body, messageProps);
		if (this.afterReceivePostProcessors != null) {
			for (MessagePostProcessor processor : this.afterReceivePostProcessors) {
				message = processor.postProcessMessage(message);
			}
		}
		return message;
	}

	private MessageConverter getRequiredMessageConverter() throws IllegalStateException {
		MessageConverter converter = getMessageConverter();
		if (converter == null) {
			throw new AmqpIllegalStateException(
					"No 'messageConverter' specified. Check configuration of RabbitTemplate.");
		}
		return converter;
	}

	private SmartMessageConverter getRequiredSmartMessageConverter() throws IllegalStateException {
		MessageConverter converter = getRequiredMessageConverter();
		Assert.state(converter instanceof SmartMessageConverter,
				"template's message converter must be a SmartMessageConverter");
		return (SmartMessageConverter) converter;
	}

	private String getRequiredQueue() throws IllegalStateException {
		String name = this.defaultReceiveQueue;
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
								+ "Request message does not contain reply-to property, " +
								"and no default Exchange was set.");
			}
			replyTo = new Address(this.exchange, this.routingKey);
		}
		return replyTo;
	}

	/**
	 * Add this template as a confirms listener for the provided channel.
	 * @param channel the channel.
	 * @since 2.0
	 */
	public void addListener(Channel channel) {
		if (channel instanceof PublisherCallbackChannel) {
			PublisherCallbackChannel publisherCallbackChannel = (PublisherCallbackChannel) channel;
			Channel key = channel instanceof ChannelProxy ? ((ChannelProxy) channel).getTargetChannel() : channel;
			if (this.publisherConfirmChannels.putIfAbsent(key, this) == null) {
				publisherCallbackChannel.addListener(this);
				if (logger.isDebugEnabled()) {
					logger.debug("Added publisher confirm channel: " + channel + " to map, size now "
							+ this.publisherConfirmChannels.size());
				}
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
			this.confirmCallback
					.confirm(pendingConfirm.getCorrelationData(), ack, pendingConfirm.getCause()); // NOSONAR never null
		}
	}

	@Override
	public void handleReturn(int replyCode,
			String replyText,
			String exchange,
			String routingKey,
			BasicProperties properties,
			byte[] body) {

		ReturnCallback callback = this.returnCallback;
		if (callback == null) {
			Object messageTagHeader = properties.getHeaders().remove(RETURN_CORRELATION_KEY);
			if (messageTagHeader != null) {
				String messageTag = messageTagHeader.toString();
				final PendingReply pendingReply = this.replyHolder.get(messageTag);
				if (pendingReply != null) {
					callback = (message, replyCode1, replyText1, exchange1, routingKey1) ->
							pendingReply.returned(new AmqpMessageReturnedException("Message returned",
									message, replyCode1, replyText1, exchange1, routingKey1));
				}
				else if (logger.isWarnEnabled()) {
					logger.warn("Returned request message but caller has timed out");
				}
			}
			else if (logger.isWarnEnabled()) {
				logger.warn("Returned message but no callback available");
			}
		}
		if (callback != null) {
			properties.getHeaders().remove(PublisherCallbackChannel.RETURN_LISTENER_CORRELATION_KEY);
			MessageProperties messageProperties = this.messagePropertiesConverter.toMessageProperties(
					properties, null, this.encoding);
			Message returnedMessage = new Message(body, messageProperties);
			callback.returnedMessage(returnedMessage,
					replyCode, replyText, exchange, routingKey);
		}
	}

	@Override
	public boolean isConfirmListener() {
		return this.confirmCallback != null;
	}

	@Override
	public boolean isReturnListener() {
		return true;
	}

	@Override
	public void revoke(Channel channel) {
		this.publisherConfirmChannels.remove(channel);
		if (logger.isDebugEnabled()) {
			logger.debug("Removed publisher confirm channel: " + channel + " from map, size now "
					+ this.publisherConfirmChannels.size());
		}
	}

	@Override
	public String getUUID() {
		return this.uuid;
	}

	@Override
	public void onMessage(Message message) {
		if (logger.isTraceEnabled()) {
			logger.trace("Message received " + message);
		}
		String messageTag;
		if (this.correlationKey == null) { // using standard correlationId property
			messageTag = message.getMessageProperties().getCorrelationId();
		}
		else {
			messageTag = (String) message.getMessageProperties()
					.getHeaders().get(this.correlationKey);
		}
		if (messageTag == null) {
			throw new AmqpRejectAndDontRequeueException("No correlation header in reply");
		}

		PendingReply pendingReply = this.replyHolder.get(messageTag);
		if (pendingReply == null) {
			if (logger.isWarnEnabled()) {
				logger.warn("Reply received after timeout for " + messageTag);
			}
			throw new AmqpRejectAndDontRequeueException("Reply received after timeout");
		}
		else {
			restoreProperties(message, pendingReply);
			pendingReply.reply(message);
		}
	}

	private void restoreProperties(Message message, PendingReply pendingReply) {
		if (!this.userCorrelationId) {
			// Restore the inbound correlation data
			String savedCorrelation = pendingReply.getSavedCorrelation();
			if (this.correlationKey == null) {
				message.getMessageProperties().setCorrelationId(savedCorrelation);
			}
			else {
				if (savedCorrelation != null) {
					message.getMessageProperties().setHeader(this.correlationKey, savedCorrelation);
				}
				else {
					message.getMessageProperties().getHeaders().remove(this.correlationKey);
				}
			}
		}
		// Restore any inbound replyTo
		String savedReplyTo = pendingReply.getSavedReplyTo();
		message.getMessageProperties().setReplyTo(savedReplyTo);
		if (logger.isDebugEnabled() && savedReplyTo != null) {
			logger.debug("Restored replyTo to " + savedReplyTo);
		}
	}

	private DefaultConsumer createConsumer(final String queueName, Channel channel,
			CompletableFuture<Delivery> future, long timeoutMillis) throws IOException, TimeoutException,
			InterruptedException {

		channel.basicQos(1);
		final CountDownLatch latch = new CountDownLatch(1);
		DefaultConsumer consumer = new TemplateConsumer(channel) {

			@Override
			public void handleCancel(String consumerTag) {
				future.completeExceptionally(new ConsumerCancelledException());
			}

			@Override
			public void handleConsumeOk(String consumerTag) {
				super.handleConsumeOk(consumerTag);
				latch.countDown();
			}

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) {
				future.complete(new Delivery(consumerTag, envelope, properties, body, queueName));
			}

		};
		channel.basicConsume(queueName, consumer);
		if (!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
			if (channel instanceof ChannelProxy) {
				((ChannelProxy) channel).getTargetChannel().close();
			}
			future.completeExceptionally(
					new ConsumeOkNotReceivedException("Blocking receive, consumer failed to consume within "
							+ timeoutMillis + " ms: " + consumer));
			RabbitUtils.setPhysicalCloseRequired(channel, true);
		}
		return consumer;
	}

	private static class PendingReply {

		@Nullable
		private volatile String savedReplyTo;

		@Nullable
		private volatile String savedCorrelation;

		private final CompletableFuture<Message> future = new CompletableFuture<>();

		@Nullable
		public String getSavedReplyTo() {
			return this.savedReplyTo;
		}

		public void setSavedReplyTo(@Nullable String savedReplyTo) {
			this.savedReplyTo = savedReplyTo;
		}

		@Nullable
		public String getSavedCorrelation() {
			return this.savedCorrelation;
		}

		public void setSavedCorrelation(@Nullable String savedCorrelation) {
			this.savedCorrelation = savedCorrelation;
		}

		public Message get() throws InterruptedException {
			try {
				return this.future.get();
			}
			catch (ExecutionException e) {
				throw RabbitExceptionTranslator.convertRabbitAccessException(e.getCause()); // NOSONAR lost stack trace
			}
		}

		@Nullable
		public Message get(long timeout, TimeUnit unit) throws InterruptedException {
			try {
				return this.future.get(timeout, unit);
			}
			catch (ExecutionException e) {
				throw RabbitExceptionTranslator.convertRabbitAccessException(e.getCause()); // NOSONAR lost stack trace
			}
			catch (TimeoutException e) {
				return null;
			}
		}

		public void reply(Message reply) {
			this.future.complete(reply);
		}

		public void returned(AmqpMessageReturnedException e) {
			completeExceptionally(e);
		}

		public void completeExceptionally(Throwable t) {
			this.future.completeExceptionally(t);
		}

	}

	/**
	 * Adds {@link #toString()} to the {@link DefaultConsumer}.
	 * @since 2.0
	 */
	protected abstract static class TemplateConsumer extends DefaultConsumer {

		public TemplateConsumer(Channel channel) {
			super(channel);
		}

		@Override
		public String toString() {
			return "TemplateConsumer [channel=" + this.getChannel() + ", consumerTag=" + this.getConsumerTag() + "]";
		}

	}

	/**
	 * A callback for publisher confirmations.
	 *
	 */
	@FunctionalInterface
	public interface ConfirmCallback {

		/**
		 * Confirmation callback.
		 * @param correlationData correlation data for the callback.
		 * @param ack true for ack, false for nack
		 * @param cause An optional cause, for nack, when available, otherwise null.
		 */
		void confirm(@Nullable CorrelationData correlationData, boolean ack, @Nullable String cause);

	}

	/**
	 * A callback for returned messages.
	 *
	 */
	@FunctionalInterface
	public interface ReturnCallback {

		/**
		 * Returned message callback.
		 * @param message the returned message.
		 * @param replyCode the reply code.
		 * @param replyText the reply text.
		 * @param exchange the exchange.
		 * @param routingKey the routing key.
		 */
		void returnedMessage(Message message, int replyCode, String replyText, String exchange, String routingKey);

	}

}
