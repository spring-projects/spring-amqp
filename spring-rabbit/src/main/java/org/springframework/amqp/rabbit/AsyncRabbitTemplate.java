/*
 * Copyright 2022-2025 the original author or authors.
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

package org.springframework.amqp.rabbit;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rabbitmq.client.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpMessageReturnedException;
import org.springframework.amqp.core.AsyncAmqpTemplate;
import org.springframework.amqp.core.Correlation;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.connection.PublisherCallbackChannel;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnsCallback;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SmartMessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.expression.Expression;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Provides asynchronous send and receive operations returning a {@link CompletableFuture}
 * allowing the caller to obtain the reply later, using {@code get()} or a callback.
 * <p>
 * When confirms are enabled, the future has a confirm property which is itself a
 * {@link CompletableFuture}. If the reply is received before the publisher confirm,
 * the confirm is discarded since the reply implicitly indicates the message was
 * published.
 * <p>
 * Returned (undeliverable) request messages are presented as a
 * {@link AmqpMessageReturnedException} cause of an
 * {@link java.util.concurrent.ExecutionException}.
 * <p>
 * Internally, the template uses a {@link RabbitTemplate} and an
 * {@link AbstractMessageListenerContainer} either provided or constructed internally
 * (a {@link SimpleMessageListenerContainer}).
 * If an external {@link RabbitTemplate} is provided and confirms/returns are enabled,
 * it must not previously have had callbacks registered because this object needs to
 * be the callback.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author FengYang Su
 * @author Ngoc Nhan
 * @author Ben Efrati
 *
 * @since 1.6
 */
public class AsyncRabbitTemplate implements AsyncAmqpTemplate, ChannelAwareMessageListener, ReturnsCallback,
		ConfirmCallback, BeanNameAware, SmartLifecycle {

	public static final int DEFAULT_RECEIVE_TIMEOUT = 30000;

	private final Log logger = LogFactory.getLog(this.getClass());

	private final Lock lock = new ReentrantLock();

	private final RabbitTemplate template;

	private final @Nullable AbstractMessageListenerContainer container;

	private final @Nullable DirectReplyToMessageListenerContainer directReplyToContainer;

	private final @Nullable String replyAddress;

	private final ConcurrentMap<String, RabbitFuture<?>> pending = new ConcurrentHashMap<>();

	private final CorrelationMessagePostProcessor<?> messagePostProcessor = new CorrelationMessagePostProcessor<>();

	private volatile boolean running;

	private volatile boolean enableConfirms;

	private volatile long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

	private int phase;

	private boolean autoStartup = true;

	@SuppressWarnings("NullAway.Init")
	private String beanName;

	@SuppressWarnings("NullAway.Init")
	private TaskScheduler taskScheduler;

	private boolean internalTaskScheduler = true;

	/**
	 * Construct an instance using the provided arguments. Replies will be
	 * routed to the default exchange using the reply queue name as the routing
	 * key.
	 * @param connectionFactory the connection factory.
	 * @param exchange the default exchange to which requests will be sent.
	 * @param routingKey the default routing key.
	 * @param replyQueue the name of the reply queue to listen for replies.
	 */
	public AsyncRabbitTemplate(ConnectionFactory connectionFactory, String exchange, String routingKey,
			String replyQueue) {

		this(connectionFactory, exchange, routingKey, replyQueue, null);
	}

	/**
	 * Construct an instance using the provided arguments. If 'replyAddress' is null,
	 * replies will be routed to the default exchange using the reply queue name as the
	 * routing key. Otherwise, it should have the form exchange/routingKey and must
	 * cause messages to be routed to the reply queue.
	 * @param connectionFactory the connection factory.
	 * @param exchange the default exchange to which requests will be sent.
	 * @param routingKey the default routing key.
	 * @param replyQueue the name of the reply queue to listen for replies.
	 * @param replyAddress the reply address (exchange/routingKey).
	 */
	@SuppressWarnings("this-escape")
	public AsyncRabbitTemplate(ConnectionFactory connectionFactory, @Nullable String exchange, String routingKey,
			String replyQueue, @Nullable String replyAddress) {

		Assert.notNull(connectionFactory, "'connectionFactory' cannot be null");
		Assert.notNull(routingKey, "'routingKey' cannot be null");
		Assert.notNull(replyQueue, "'replyQueue' cannot be null");
		this.template = new RabbitTemplate(connectionFactory);
		this.template.setExchange(exchange == null ? "" : exchange);
		this.template.setRoutingKey(routingKey);
		this.container = new SimpleMessageListenerContainer(connectionFactory);
		JavaUtils.INSTANCE
				.acceptIfNotNull(this.template.getAfterReceivePostProcessors(),
						(value) -> this.container.setAfterReceivePostProcessors(
								value.toArray(new MessagePostProcessor[0])));
		this.container.setQueueNames(replyQueue);
		this.container.setMessageListener(this);
		this.container.afterPropertiesSet();
		this.directReplyToContainer = null;
		this.replyAddress = Objects.requireNonNullElse(replyAddress, replyQueue);

	}

	/**
	 * Construct an instance using the provided arguments. The first queue the container
	 * is configured to listen to will be used as the reply queue. Replies will be
	 * routed using the default exchange with that queue name as the routing key.
	 * @param template a {@link RabbitTemplate}
	 * @param container a {@link AbstractMessageListenerContainer}.
	 */
	public AsyncRabbitTemplate(RabbitTemplate template, AbstractMessageListenerContainer container) {
		this(template, container, null);
	}

	/**
	 * Construct an instance using the provided arguments. The first queue the container
	 * is configured to listen to will be used as the reply queue. If 'replyAddress' is
	 * null, replies will be routed using the default exchange with that queue name as the
	 * routing key. Otherwise, it should have the form exchange/routingKey and must
	 * cause messages to be routed to the reply queue.
	 * @param template a {@link RabbitTemplate}.
	 * @param container a {@link AbstractMessageListenerContainer}.
	 * @param replyAddress the reply address.
	 */
	@SuppressWarnings("this-escape")
	public AsyncRabbitTemplate(RabbitTemplate template, AbstractMessageListenerContainer container,
			@Nullable String replyAddress) {

		Assert.notNull(template, "'template' cannot be null");
		Assert.notNull(container, "'container' cannot be null");
		this.template = template;
		this.container = container;
		this.container.setMessageListener(this);
		this.directReplyToContainer = null;
		this.replyAddress = Objects.requireNonNullElseGet(replyAddress, () -> container.getQueueNames()[0]);
	}

	/**
	 * Construct an instance using the provided arguments. "Direct replyTo" is used for
	 * replies.
	 * @param connectionFactory the connection factory.
	 * @param exchange the default exchange to which requests will be sent.
	 * @param routingKey the default routing key.
	 * @since 2.0
	 */
	public AsyncRabbitTemplate(ConnectionFactory connectionFactory, @Nullable String exchange, String routingKey) {
		this(new RabbitTemplate(connectionFactory));
		Assert.notNull(routingKey, "'routingKey' cannot be null");
		this.template.setExchange(exchange == null ? "" : exchange);
		this.template.setRoutingKey(routingKey);
	}

	/**
	 * Construct an instance using the provided arguments. "Direct replyTo" is used for
	 * replies.
	 * @param template a {@link RabbitTemplate}
	 * @since 2.0
	 */
	@SuppressWarnings("this-escape")
	public AsyncRabbitTemplate(RabbitTemplate template) {
		Assert.notNull(template, "'template' cannot be null");
		this.template = template;
		this.container = null;
		this.replyAddress = null;
		this.directReplyToContainer = new DirectReplyToMessageListenerContainer(this.template.getConnectionFactory());
		JavaUtils.INSTANCE
				.acceptIfNotNull(template.getAfterReceivePostProcessors(),
						(value) -> this.directReplyToContainer.setAfterReceivePostProcessors(
								value.toArray(new MessagePostProcessor[0])));
		this.directReplyToContainer.setMessageListener(this);
	}

	/**
	 * @param autoStartup true for auto start.
	 * @see #isAutoStartup()
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * @param phase the phase.
	 * @see #getPhase()
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * Set to true to enable the receipt of returned messages that cannot be delivered
	 * in the form of a {@link AmqpMessageReturnedException}.
	 * @param mandatory true to enable returns.
	 */
	public void setMandatory(boolean mandatory) {
		this.template.setReturnsCallback(this);
		this.template.setMandatory(mandatory);
	}

	/**
	 * @param mandatoryExpression a SpEL {@link Expression} to evaluate against each request
	 * message. The result of the evaluation must be a {@code boolean} value.
	 * @since 2.0
	 */
	public void setMandatoryExpression(Expression mandatoryExpression) {
		this.template.setReturnsCallback(this);
		this.template.setMandatoryExpression(mandatoryExpression);
	}

	/**
	 * @param mandatoryExpression a SpEL {@link Expression} to evaluate against each request
	 * message. The result of the evaluation must be a {@code boolean} value.
	 * @since 2.0
	 */
	public void setMandatoryExpressionString(String mandatoryExpression) {
		this.template.setReturnsCallback(this);
		this.template.setMandatoryExpressionString(mandatoryExpression);
	}

	/**
	 * Set to true to enable publisher confirms. When enabled, the {@link RabbitFuture}
	 * returned by the send and receive operation will have a
	 * {@code CompletableFuture<Boolean>} in its {@code confirm} property.
	 * @param enableConfirms true to enable publisher confirms.
	 */
	public void setEnableConfirms(boolean enableConfirms) {
		this.enableConfirms = enableConfirms;
		if (enableConfirms) {
			this.template.setConfirmCallback(this);
		}
	}

	public String getBeanName() {
		return this.beanName;
	}

	@Override
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	/**
	 * @return a reference to the underlying connection factory in the
	 * {@link RabbitTemplate}.
	 */
	public ConnectionFactory getConnectionFactory() {
		return this.template.getConnectionFactory();
	}

	/**
	 * Set the receive timeout - the future returned by the send and receive
	 * methods will be canceled when this timeout expires. {@code <= 0} means
	 * futures never expire. Beware that this will cause a memory leak if a
	 * reply is not received. Default: 30000 (30 seconds).
	 * @param receiveTimeout the timeout in milliseconds.
	 */
	public void setReceiveTimeout(long receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	/**
	 * Set the task scheduler to expire timed out futures.
	 * @param taskScheduler the task scheduler
	 * @see #setReceiveTimeout(long)
	 */
	public void setTaskScheduler(TaskScheduler taskScheduler) {
		Assert.notNull(taskScheduler, "'taskScheduler' cannot be null");
		this.lock.lock();
		try {
			this.internalTaskScheduler = false;
			this.taskScheduler = taskScheduler;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * @return a reference to the underlying {@link RabbitTemplate}'s
	 * {@link MessageConverter}.
	 */
	public MessageConverter getMessageConverter() {
		return this.template.getMessageConverter();
	}

	/**
	 * Return the underlying {@link RabbitTemplate} used for sending.
	 * @return the template.
	 * @since 2.2
	 */
	public RabbitTemplate getRabbitTemplate() {
		return this.template;
	}

	@Override
	public RabbitMessageFuture sendAndReceive(Message message) {
		return sendAndReceive(this.template.getExchange(), this.template.getRoutingKey(), message);
	}

	@Override
	public RabbitMessageFuture sendAndReceive(String routingKey, Message message) {
		return sendAndReceive(this.template.getExchange(), routingKey, message);
	}

	@Override
	public RabbitMessageFuture sendAndReceive(String exchange, String routingKey, Message message) {
		String correlationId = getOrSetCorrelationIdAndSetReplyTo(message, null);
		RabbitMessageFuture future = new RabbitMessageFuture(correlationId, message, this::canceler,
				this::timeoutTask);
		CorrelationData correlationData = null;
		if (this.enableConfirms) {
			correlationData = new CorrelationData(correlationId);
			future.setConfirm(new CompletableFuture<>());
		}
		this.pending.put(correlationId, future);
		if (this.container != null) {
			this.template.send(exchange, routingKey, message, correlationData);
		}
		else {
			Assert.notNull(this.directReplyToContainer, "'directReplyToContainer' cannot be null");
			ChannelHolder channelHolder = this.directReplyToContainer.getChannelHolder();
			future.setChannelHolder(channelHolder);
			sendDirect(channelHolder.getChannel(), exchange, routingKey, message, correlationData);
		}
		future.startTimer();
		return future;
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceive(Object object) {
		return convertSendAndReceive(this.template.getExchange(), this.template.getRoutingKey(), object, null);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceive(String routingKey, Object object) {
		return convertSendAndReceive(this.template.getExchange(), routingKey, object, null);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceive(String exchange, String routingKey, Object object) {
		return convertSendAndReceive(exchange, routingKey, object, null);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceive(Object object,
			MessagePostProcessor messagePostProcessor) {
		return convertSendAndReceive(this.template.getExchange(), this.template.getRoutingKey(), object,
				messagePostProcessor);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceive(String routingKey, Object object,
			MessagePostProcessor messagePostProcessor) {

		return convertSendAndReceive(this.template.getExchange(), routingKey, object, messagePostProcessor);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceive(String exchange, String routingKey, Object object,
			@Nullable MessagePostProcessor messagePostProcessor) {

		return convertSendAndReceive(exchange, routingKey, object, messagePostProcessor, null);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceiveAsType(Object object,
			ParameterizedTypeReference<C> responseType) {

		return convertSendAndReceiveAsType(this.template.getExchange(), this.template.getRoutingKey(), object,
				null, responseType);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceiveAsType(String routingKey, Object object,
			ParameterizedTypeReference<C> responseType) {

		return convertSendAndReceiveAsType(this.template.getExchange(), routingKey, object, null, responseType);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceiveAsType(String exchange, String routingKey, Object object,
			ParameterizedTypeReference<C> responseType) {

		return convertSendAndReceiveAsType(exchange, routingKey, object, null, responseType);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceiveAsType(Object object,
			MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<C> responseType) {

		return convertSendAndReceiveAsType(this.template.getExchange(), this.template.getRoutingKey(), object,
				messagePostProcessor, responseType);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceiveAsType(String routingKey, Object object,
			@Nullable MessagePostProcessor messagePostProcessor, @Nullable ParameterizedTypeReference<C> responseType) {

		return convertSendAndReceiveAsType(this.template.getExchange(), routingKey, object, messagePostProcessor,
				responseType);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceiveAsType(String exchange, String routingKey, Object object,
			@Nullable MessagePostProcessor messagePostProcessor, @Nullable ParameterizedTypeReference<C> responseType) {

		Assert.state(this.template.getMessageConverter() instanceof SmartMessageConverter,
				"template's message converter must be a SmartMessageConverter");
		return convertSendAndReceive(exchange, routingKey, object, messagePostProcessor, responseType);
	}

	private <C> RabbitConverterFuture<C> convertSendAndReceive(String exchange, String routingKey, Object object,
			@Nullable MessagePostProcessor messagePostProcessor, @Nullable ParameterizedTypeReference<C> responseType) {

		AsyncCorrelationData<C> correlationData = new AsyncCorrelationData<>(messagePostProcessor, responseType,
				this.enableConfirms);
		if (this.container != null) {
			this.template.convertAndSend(exchange, routingKey, object, this.messagePostProcessor, correlationData);
		}
		else {
			MessageConverter converter = this.template.getMessageConverter();
			Message message = converter.toMessage(object, new MessageProperties());
			this.messagePostProcessor.postProcessMessage(message, correlationData,
					this.template.nullSafeExchange(exchange), this.template.nullSafeRoutingKey(routingKey));
			@SuppressWarnings("NullAway") // Dataflow analysis limitation
			ChannelHolder channelHolder = this.directReplyToContainer.getChannelHolder();
			correlationData.future.setChannelHolder(channelHolder);
			sendDirect(channelHolder.getChannel(), exchange, routingKey, message, correlationData);
		}
		RabbitConverterFuture<C> future = correlationData.future;
		future.startTimer();
		return future;
	}

	private void sendDirect(Channel channel, String exchange, String routingKey, Message message,
			@Nullable CorrelationData correlationData) {

		message.getMessageProperties().setReplyTo(Address.AMQ_RABBITMQ_REPLY_TO);
		try {
			if (channel instanceof PublisherCallbackChannel) {
				this.template.addListener(channel);
			}
			this.template.doSend(channel, exchange, routingKey, message, this.template.isMandatoryFor(message),
					correlationData);
		}
		catch (Exception e) {
			throw new AmqpException("Failed to send request", e);
		}
	}

	@Override
	public void start() {
		this.lock.lock();
		try {
			if (!this.running) {
				if (this.internalTaskScheduler) {
					ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
					scheduler.setThreadNamePrefix(getBeanName() + "-");
					scheduler.afterPropertiesSet();
					this.taskScheduler = scheduler;
				}
				if (this.container != null) {
					this.container.start();
				}
				if (this.directReplyToContainer != null) {
					this.directReplyToContainer.setTaskScheduler(this.taskScheduler);
					this.directReplyToContainer.start();
				}
			}
			this.running = true;
		}
		finally {
			this.lock.unlock();
		}
	}

	@Override
	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	public void stop() {
		this.lock.lock();
		try {
			if (this.running) {
				if (this.container != null) {
					this.container.stop();
				}
				if (this.directReplyToContainer != null) {
					this.directReplyToContainer.stop();
				}
				for (RabbitFuture<?> future : this.pending.values()) {
					future.setNackCause("AsyncRabbitTemplate was stopped while waiting for reply");
					future.cancel(true);
				}
				if (this.internalTaskScheduler) {
					((ThreadPoolTaskScheduler) this.taskScheduler).destroy();
					this.taskScheduler = null;
				}
			}
			this.running = false;
		}
		finally {
			this.lock.unlock();
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onMessage(Message message, @Nullable Channel channel) {
		MessageProperties messageProperties = message.getMessageProperties();
		String correlationId = messageProperties.getCorrelationId();
		if (StringUtils.hasText(correlationId)) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("onMessage: " + message);
			}
			RabbitFuture<?> future = this.pending.remove(correlationId);
			if (future != null) {
				if (future instanceof RabbitConverterFuture) {
					MessageConverter messageConverter = this.template.getMessageConverter();
					RabbitConverterFuture<Object> rabbitFuture = (RabbitConverterFuture<Object>) future;
					try {
						Object converted = rabbitFuture.getReturnType() != null
								&& messageConverter instanceof SmartMessageConverter smart
								? smart.fromMessage(message,
								rabbitFuture.getReturnType())
								: messageConverter.fromMessage(message);
						rabbitFuture.complete(converted);
					}
					catch (MessageConversionException e) {
						rabbitFuture.completeExceptionally(e);
					}
				}
				else {
					((RabbitMessageFuture) future).complete(message);
				}
			}
			else {
				if (this.logger.isWarnEnabled()) {
					this.logger.warn("No pending reply - perhaps timed out: " + message);
				}
			}
		}
	}

	@Override
	public void returnedMessage(ReturnedMessage returned) {
		MessageProperties messageProperties = returned.getMessage().getMessageProperties();
		String correlationId = messageProperties.getCorrelationId();
		if (StringUtils.hasText(correlationId)) {
			RabbitFuture<?> future = this.pending.remove(correlationId);
			if (future != null) {
				future.completeExceptionally(new AmqpMessageReturnedException("Message returned", returned));
			}
			else {
				if (this.logger.isWarnEnabled()) {
					this.logger
							.warn("No pending reply - perhaps timed out? Message returned: " + returned.getMessage());
				}
			}
		}
	}

	@Override
	public void confirm(@Nullable CorrelationData correlationData, boolean ack, @Nullable String cause) {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Confirm: " + correlationData + ", ack=" + ack
					+ (cause == null ? "" : (", cause: " + cause)));
		}
		Assert.notNull(correlationData, "'correlationData' must not be null");
		String correlationId = correlationData.getId();
		RabbitFuture<?> future = this.pending.get(correlationId);
		if (future != null) {
			future.setNackCause(cause);
			future.getConfirm().complete(ack);
		}
		else {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Confirm: " + correlationData + ", ack=" + ack
						+ (cause == null ? "" : (", cause: " + cause))
						+ " no pending future - either canceled or the reply is already received");
			}
		}
	}

	private String getOrSetCorrelationIdAndSetReplyTo(Message message,
			@Nullable AsyncCorrelationData<?> correlationData) {

		MessageProperties messageProperties = message.getMessageProperties();
		Assert.notNull(messageProperties, "the message properties cannot be null");
		String correlationId = messageProperties.getCorrelationId();
		if (!StringUtils.hasText(correlationId)) {
			correlationId = correlationData != null ? correlationData.getId() : UUID.randomUUID().toString();
			messageProperties.setCorrelationId(correlationId);
			Assert.isNull(messageProperties.getReplyTo(), "'replyTo' property must be null");
		}
		messageProperties.setReplyTo(this.replyAddress);
		return correlationId;
	}

	private void canceler(String correlationId, @Nullable ChannelHolder channelHolder) {
		this.pending.remove(correlationId);
		if (channelHolder != null && this.directReplyToContainer != null) {
			this.directReplyToContainer
					.releaseConsumerFor(channelHolder, false, null); // NOSONAR
		}
	}

	private @Nullable ScheduledFuture<?> timeoutTask(RabbitFuture<?> future) {
		if (this.receiveTimeout > 0) {
			this.lock.lock();
			try {
				if (!this.running) {
					this.pending.remove(future.getCorrelationId());
					throw new IllegalStateException("'AsyncRabbitTemplate' must be started.");
				}
				return this.taskScheduler.schedule(
						new TimeoutTask(future, this.pending, this.directReplyToContainer),
						Instant.now().plusMillis(this.receiveTimeout));
			}
			finally {
				this.lock.unlock();
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + ": " + this.beanName;
	}

	private final class CorrelationMessagePostProcessor<C> implements MessagePostProcessor {

		CorrelationMessagePostProcessor() {
		}

		@Override
		public Message postProcessMessage(Message message) throws AmqpException {
			throw new UnsupportedOperationException();
		}

		@SuppressWarnings("unchecked")
		@Override
		public Message postProcessMessage(Message message, @Nullable Correlation correlation) throws AmqpException {
			Message messageToSend = message;
			AsyncCorrelationData<C> correlationData = (AsyncCorrelationData<C>) correlation;
			Assert.notNull(correlationData, "correlationData cannot be null");
			if (correlationData.userPostProcessor != null) {
				messageToSend = correlationData.userPostProcessor.postProcessMessage(message);
			}
			String correlationId = getOrSetCorrelationIdAndSetReplyTo(messageToSend, correlationData);
			correlationData.future = new RabbitConverterFuture<>(correlationId, message,
					AsyncRabbitTemplate.this::canceler, AsyncRabbitTemplate.this::timeoutTask);
			if (correlationData.enableConfirms) {
				correlationData.setId(correlationId);
				correlationData.future.setConfirm(new CompletableFuture<>());
			}
			correlationData.future.setReturnType(correlationData.returnType);
			AsyncRabbitTemplate.this.pending.put(correlationId, correlationData.future);
			return messageToSend;
		}

	}

	private static class AsyncCorrelationData<C> extends CorrelationData {

		final @Nullable MessagePostProcessor userPostProcessor; // NOSONAR

		final @Nullable ParameterizedTypeReference<C> returnType; // NOSONAR

		final boolean enableConfirms; // NOSONAR

		@SuppressWarnings("NullAway.Init")
		volatile RabbitConverterFuture<C> future; // NOSONAR

		AsyncCorrelationData(@Nullable MessagePostProcessor userPostProcessor,
				@Nullable ParameterizedTypeReference<C> returnType, boolean enableConfirms) {

			this.userPostProcessor = userPostProcessor;
			this.returnType = returnType;
			this.enableConfirms = enableConfirms;
		}

	}

}
