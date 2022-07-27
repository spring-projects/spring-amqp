/*
 * Copyright 2022 the original author or authors.
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

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpMessageReturnedException;
import org.springframework.amqp.core.AmqpReplyTimeoutException;
import org.springframework.amqp.core.AsyncAmqpTemplate2;
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
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SmartMessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.expression.Expression;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Channel;

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
 *
 * @since 1.6
 */
public class AsyncRabbitTemplate2 implements AsyncAmqpTemplate2, ChannelAwareMessageListener, ReturnsCallback,
		ConfirmCallback, BeanNameAware, SmartLifecycle {

	public static final int DEFAULT_RECEIVE_TIMEOUT = 30000;

	private final Log logger = LogFactory.getLog(this.getClass());

	private final RabbitTemplate template;

	private final AbstractMessageListenerContainer container;

	private final DirectReplyToMessageListenerContainer directReplyToContainer;

	private final String replyAddress;

	private final ConcurrentMap<String, RabbitFuture2<?>> pending = new ConcurrentHashMap<>();

	private final CorrelationMessagePostProcessor<?> messagePostProcessor = new CorrelationMessagePostProcessor<>();

	private volatile boolean running;

	private volatile boolean enableConfirms;

	private volatile long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

	private int phase;

	private boolean autoStartup = true;

	private String beanName;

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
	public AsyncRabbitTemplate2(ConnectionFactory connectionFactory, String exchange, String routingKey,
			String replyQueue) {
		this(connectionFactory, exchange, routingKey, replyQueue, null);
	}

	/**
	 * Construct an instance using the provided arguments. If 'replyAddress' is null,
	 * replies will be routed to the default exchange using the reply queue name as the
	 * routing key. Otherwise it should have the form exchange/routingKey and must
	 * cause messages to be routed to the reply queue.
	 * @param connectionFactory the connection factory.
	 * @param exchange the default exchange to which requests will be sent.
	 * @param routingKey the default routing key.
	 * @param replyQueue the name of the reply queue to listen for replies.
	 * @param replyAddress the reply address (exchange/routingKey).
	 */
	public AsyncRabbitTemplate2(ConnectionFactory connectionFactory, String exchange, String routingKey,
			String replyQueue, String replyAddress) {
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
		if (replyAddress == null) {
			this.replyAddress = replyQueue;
		}
		else {
			this.replyAddress = replyAddress;
		}

	}

	/**
	 * Construct an instance using the provided arguments. The first queue the container
	 * is configured to listen to will be used as the reply queue. Replies will be
	 * routed using the default exchange with that queue name as the routing key.
	 * @param template a {@link RabbitTemplate}
	 * @param container a {@link AbstractMessageListenerContainer}.
	 */
	public AsyncRabbitTemplate2(RabbitTemplate template, AbstractMessageListenerContainer container) {
		this(template, container, null);
	}

	/**
	 * Construct an instance using the provided arguments. The first queue the container
	 * is configured to listen to will be used as the reply queue. If 'replyAddress' is
	 * null, replies will be routed using the default exchange with that queue name as the
	 * routing key. Otherwise it should have the form exchange/routingKey and must
	 * cause messages to be routed to the reply queue.
	 * @param template a {@link RabbitTemplate}.
	 * @param container a {@link AbstractMessageListenerContainer}.
	 * @param replyAddress the reply address.
	 */
	public AsyncRabbitTemplate2(RabbitTemplate template, AbstractMessageListenerContainer container,
			String replyAddress) {
		Assert.notNull(template, "'template' cannot be null");
		Assert.notNull(container, "'container' cannot be null");
		this.template = template;
		this.container = container;
		this.container.setMessageListener(this);
		this.directReplyToContainer = null;
		if (replyAddress == null) {
			this.replyAddress = container.getQueueNames()[0];
		}
		else {
			this.replyAddress = replyAddress;
		}
	}

	/**
	 * Construct an instance using the provided arguments. "Direct replyTo" is used for
	 * replies.
	 * @param connectionFactory the connection factory.
	 * @param exchange the default exchange to which requests will be sent.
	 * @param routingKey the default routing key.
	 * @since 2.0
	 */
	public AsyncRabbitTemplate2(ConnectionFactory connectionFactory, String exchange, String routingKey) {
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
	public AsyncRabbitTemplate2(RabbitTemplate template) {
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
	 * Set to true to enable publisher confirms. When enabled, the {@link RabbitFuture2}
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
	public synchronized void setTaskScheduler(TaskScheduler taskScheduler) {
		Assert.notNull(taskScheduler, "'taskScheduler' cannot be null");
		this.internalTaskScheduler = false;
		this.taskScheduler = taskScheduler;
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
	public RabbitMessageFuture2 sendAndReceive(Message message) {
		return sendAndReceive(this.template.getExchange(), this.template.getRoutingKey(), message);
	}

	@Override
	public RabbitMessageFuture2 sendAndReceive(String routingKey, Message message) {
		return sendAndReceive(this.template.getExchange(), routingKey, message);
	}

	@Override
	public RabbitMessageFuture2 sendAndReceive(String exchange, String routingKey, Message message) {
		String correlationId = getOrSetCorrelationIdAndSetReplyTo(message, null);
		RabbitMessageFuture2 future = new RabbitMessageFuture2(correlationId, message);
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
			ChannelHolder channelHolder = this.directReplyToContainer.getChannelHolder();
			future.setChannelHolder(channelHolder);
			sendDirect(channelHolder.getChannel(), exchange, routingKey, message, correlationData);
		}
		future.startTimer();
		return future;
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceive(Object object) {
		return convertSendAndReceive(this.template.getExchange(), this.template.getRoutingKey(), object, null);
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceive(String routingKey, Object object) {
		return convertSendAndReceive(this.template.getExchange(), routingKey, object, null);
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceive(String exchange, String routingKey, Object object) {
		return convertSendAndReceive(exchange, routingKey, object, null);
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceive(Object object,
			MessagePostProcessor messagePostProcessor) {
		return convertSendAndReceive(this.template.getExchange(), this.template.getRoutingKey(), object,
				messagePostProcessor);
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceive(String routingKey, Object object,
			MessagePostProcessor messagePostProcessor) {
		return convertSendAndReceive(this.template.getExchange(), routingKey, object, messagePostProcessor);
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceive(String exchange, String routingKey, Object object,
			MessagePostProcessor messagePostProcessor) {
		return convertSendAndReceive(exchange, routingKey, object, messagePostProcessor, null);
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceiveAsType(Object object,
			ParameterizedTypeReference<C> responseType) {
		return convertSendAndReceiveAsType(this.template.getExchange(), this.template.getRoutingKey(), object,
				null, responseType);
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceiveAsType(String routingKey, Object object,
			ParameterizedTypeReference<C> responseType) {
		return convertSendAndReceiveAsType(this.template.getExchange(), routingKey, object, null, responseType);
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceiveAsType(String exchange, String routingKey, Object object,
			ParameterizedTypeReference<C> responseType) {
		return convertSendAndReceiveAsType(exchange, routingKey, object, null, responseType);
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceiveAsType(Object object,
			MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<C> responseType) {
		return convertSendAndReceiveAsType(this.template.getExchange(), this.template.getRoutingKey(), object,
				messagePostProcessor, responseType);
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceiveAsType(String routingKey, Object object,
			MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<C> responseType) {
		return convertSendAndReceiveAsType(this.template.getExchange(), routingKey, object, messagePostProcessor,
				responseType);
	}

	@Override
	public <C> RabbitConverterFuture2<C> convertSendAndReceiveAsType(String exchange, String routingKey, Object object,
			MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<C> responseType) {
		Assert.state(this.template.getMessageConverter() instanceof SmartMessageConverter,
				"template's message converter must be a SmartMessageConverter");
		return convertSendAndReceive(exchange, routingKey, object, messagePostProcessor, responseType);
	}

	private <C> RabbitConverterFuture2<C> convertSendAndReceive(String exchange, String routingKey, Object object,
			MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<C> responseType) {

		AsyncCorrelationData<C> correlationData = new AsyncCorrelationData<C>(messagePostProcessor, responseType,
				this.enableConfirms);
		if (this.container != null) {
			this.template.convertAndSend(exchange, routingKey, object, this.messagePostProcessor, correlationData);
		}
		else {
			MessageConverter converter = this.template.getMessageConverter();
			if (converter == null) {
				throw new AmqpIllegalStateException(
						"No 'messageConverter' specified. Check configuration of RabbitTemplate.");
			}
			Message message = converter.toMessage(object, new MessageProperties());
			this.messagePostProcessor.postProcessMessage(message, correlationData,
					this.template.nullSafeExchange(exchange), this.template.nullSafeRoutingKey(routingKey));
			ChannelHolder channelHolder = this.directReplyToContainer.getChannelHolder();
			correlationData.future.setChannelHolder(channelHolder);
			sendDirect(channelHolder.getChannel(), exchange, routingKey, message, correlationData);
		}
		RabbitConverterFuture2<C> future = correlationData.future;
		future.startTimer();
		return future;
	}

	private void sendDirect(Channel channel, String exchange, String routingKey, Message message,
			CorrelationData correlationData) {
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
	public synchronized void start() {
		if (!this.running) {
			if (this.internalTaskScheduler) {
				ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
				scheduler.setThreadNamePrefix(getBeanName() == null ? "asyncTemplate-" : (getBeanName() + "-"));
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

	@Override
	public synchronized void stop() {
		if (this.running) {
			if (this.container != null) {
				this.container.stop();
			}
			if (this.directReplyToContainer != null) {
				this.directReplyToContainer.stop();
			}
			for (RabbitFuture2<?> future : this.pending.values()) {
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
	public void onMessage(Message message, Channel channel) {
		MessageProperties messageProperties = message.getMessageProperties();
		if (messageProperties != null) {
			String correlationId = messageProperties.getCorrelationId();
			if (StringUtils.hasText(correlationId)) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("onMessage: " + message);
				}
				RabbitFuture2<?> future = this.pending.remove(correlationId);
				if (future != null) {
					if (future instanceof AsyncRabbitTemplate2.RabbitConverterFuture2) {
						MessageConverter messageConverter = this.template.getMessageConverter();
						RabbitConverterFuture2<Object> rabbitFuture = (RabbitConverterFuture2<Object>) future;
						Object converted = rabbitFuture.getReturnType() != null
								&& messageConverter instanceof SmartMessageConverter
								? ((SmartMessageConverter) messageConverter).fromMessage(message,
								rabbitFuture.getReturnType())
								: messageConverter.fromMessage(message);
						rabbitFuture.complete(converted);
					}
					else {
						((RabbitMessageFuture2) future).complete(message);
					}
				}
				else {
					if (this.logger.isWarnEnabled()) {
						this.logger.warn("No pending reply - perhaps timed out: " + message);
					}
				}
			}
		}
	}

	@Override
	public void returnedMessage(ReturnedMessage returned) {
		MessageProperties messageProperties = returned.getMessage().getMessageProperties();
		String correlationId = messageProperties.getCorrelationId();
		if (StringUtils.hasText(correlationId)) {
			RabbitFuture2<?> future = this.pending.remove(correlationId);
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
	public void confirm(@NonNull CorrelationData correlationData, boolean ack, @Nullable String cause) {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Confirm: " + correlationData + ", ack=" + ack
					+ (cause == null ? "" : (", cause: " + cause)));
		}
		String correlationId = correlationData.getId();
		if (correlationId != null) {
			RabbitFuture2<?> future = this.pending.get(correlationId);
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
	}

	private String getOrSetCorrelationIdAndSetReplyTo(Message message,
			@Nullable AsyncCorrelationData<?> correlationData) {

		String correlationId;
		MessageProperties messageProperties = message.getMessageProperties();
		Assert.notNull(messageProperties, "the message properties cannot be null");
		String currentCorrelationId = messageProperties.getCorrelationId();
		if (!StringUtils.hasText(currentCorrelationId)) {
			correlationId = correlationData != null ? correlationData.getId() : UUID.randomUUID().toString();
			messageProperties.setCorrelationId(correlationId);
			Assert.isNull(messageProperties.getReplyTo(), "'replyTo' property must be null");
		}
		else {
			correlationId = currentCorrelationId;
		}
		messageProperties.setReplyTo(this.replyAddress);
		return correlationId;
	}

	@Override
	public String toString() {
		return this.beanName == null ? super.toString() : (this.getClass().getSimpleName() + ": " + this.beanName);
	}

	/**
	 * Base class for {@link CompletableFuture}s returned by {@link AsyncRabbitTemplate2}.
	 * @param <T> the type.
	 * @since 1.6
	 */
	public abstract class RabbitFuture2<T> extends CompletableFuture<T> {

		private final String correlationId;

		private final Message requestMessage;

		private ScheduledFuture<?> timeoutTask;

		private volatile CompletableFuture<Boolean> confirm;

		private String nackCause;

		private ChannelHolder channelHolder;

		public RabbitFuture2(String correlationId, Message requestMessage) {
			this.correlationId = correlationId;
			this.requestMessage = requestMessage;
		}

		void setChannelHolder(ChannelHolder channel) {
			this.channelHolder = channel;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (this.timeoutTask != null) {
				this.timeoutTask.cancel(true);
			}
			AsyncRabbitTemplate2.this.pending.remove(this.correlationId);
			if (this.channelHolder != null && AsyncRabbitTemplate2.this.directReplyToContainer != null) {
				AsyncRabbitTemplate2.this.directReplyToContainer
						.releaseConsumerFor(this.channelHolder, false, null); // NOSONAR
			}
			return super.cancel(mayInterruptIfRunning);
		}

		/**
		 * When confirms are enabled contains a {@link CompletableFuture}
		 * for the confirmation.
		 * @return the future.
		 */
		public CompletableFuture<Boolean> getConfirm() {
			return this.confirm;
		}

		void setConfirm(CompletableFuture<Boolean> confirm) {
			this.confirm = confirm;
		}

		/**
		 * When confirms are enabled and a nack is received, contains
		 * the cause for the nack, if any.
		 * @return the cause.
		 */
		public String getNackCause() {
			return this.nackCause;
		}

		void setNackCause(String nackCause) {
			this.nackCause = nackCause;
		}

		void startTimer() {
			if (AsyncRabbitTemplate2.this.receiveTimeout > 0) {
				synchronized (AsyncRabbitTemplate2.this) {
					if (!AsyncRabbitTemplate2.this.running) {
						AsyncRabbitTemplate2.this.pending.remove(this.correlationId);
						throw new IllegalStateException("'AsyncRabbitTemplate' must be started.");
					}
					this.timeoutTask = AsyncRabbitTemplate2.this.taskScheduler.schedule(new TimeoutTask(),
							new Date(System.currentTimeMillis() + AsyncRabbitTemplate2.this.receiveTimeout));
				}
			}
			else {
				this.timeoutTask = null;
			}
		}

		private class TimeoutTask implements Runnable {

			@Override
			public void run() {
				AsyncRabbitTemplate2.this.pending.remove(RabbitFuture2.this.correlationId);
				if (RabbitFuture2.this.channelHolder != null
						&& AsyncRabbitTemplate2.this.directReplyToContainer != null) {
					AsyncRabbitTemplate2.this.directReplyToContainer
							.releaseConsumerFor(RabbitFuture2.this.channelHolder, false, null); // NOSONAR
				}
				completeExceptionally(
						new AmqpReplyTimeoutException("Reply timed out", RabbitFuture2.this.requestMessage));
			}

		}

	}

	/**
	 * A {@link RabbitFuture2} with a return type of {@link Message}.
	 * @since 1.6
	 */
	public class RabbitMessageFuture2 extends RabbitFuture2<Message> {

		public RabbitMessageFuture2(String correlationId, Message requestMessage) {
			super(correlationId, requestMessage);
		}

	}

	/**
	 * A {@link RabbitFuture2} with a return type of the template's
	 * generic parameter.
	 * @param <C> the type.
	 * @since 1.6
	 */
	public class RabbitConverterFuture2<C> extends RabbitFuture2<C> {

		private volatile ParameterizedTypeReference<C> returnType;

		public RabbitConverterFuture2(String correlationId, Message requestMessage) {
			super(correlationId, requestMessage);
		}

		public ParameterizedTypeReference<C> getReturnType() {
			return this.returnType;
		}

		public void setReturnType(ParameterizedTypeReference<C> returnType) {
			this.returnType = returnType;
		}

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
		public Message postProcessMessage(Message message, Correlation correlation) throws AmqpException {
			Message messageToSend = message;
			AsyncCorrelationData<C> correlationData = (AsyncCorrelationData<C>) correlation;
			if (correlationData.userPostProcessor != null) {
				messageToSend = correlationData.userPostProcessor.postProcessMessage(message);
			}
			String correlationId = getOrSetCorrelationIdAndSetReplyTo(messageToSend, correlationData);
			correlationData.future = new RabbitConverterFuture2<C>(correlationId, message);
			if (correlationData.enableConfirms) {
				correlationData.setId(correlationId);
				correlationData.future.setConfirm(new CompletableFuture<>());
			}
			correlationData.future.setReturnType(correlationData.returnType);
			AsyncRabbitTemplate2.this.pending.put(correlationId, correlationData.future);
			return messageToSend;
		}

	}

	private static class AsyncCorrelationData<C> extends CorrelationData {

		final MessagePostProcessor userPostProcessor; // NOSONAR

		final ParameterizedTypeReference<C> returnType; // NOSONAR

		final boolean enableConfirms; // NOSONAR

		volatile RabbitConverterFuture2<C> future; // NOSONAR

		AsyncCorrelationData(MessagePostProcessor userPostProcessor, ParameterizedTypeReference<C> returnType,
				boolean enableConfirms) {

			this.userPostProcessor = userPostProcessor;
			this.returnType = returnType;
			this.enableConfirms = enableConfirms;
		}

	}

}
