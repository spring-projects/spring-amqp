/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.amqp.rabbit;

import java.util.Date;
import java.util.UUID;
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
import org.springframework.amqp.core.AsyncAmqpTemplate;
import org.springframework.amqp.core.Correlation;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannel;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SmartMessageConverter;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.expression.Expression;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.rabbitmq.client.Channel;

/**
 * Provides asynchronous send and receive operations returning a {@link ListenableFuture}
 * allowing the caller to obtain the reply later, using {@code get()} or a callback.
 * <p>
 * When confirms are enabled, the future has a confirm property which is itself a
 * {@link ListenableFuture}. If the reply is received before the publisher confirm,
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
public class AsyncRabbitTemplate implements AsyncAmqpTemplate, ChannelAwareMessageListener, ReturnCallback,
		ConfirmCallback, BeanNameAware, SmartLifecycle {

	public static final int DEFAULT_RECEIVE_TIMEOUT = 30000;

	private final Log logger = LogFactory.getLog(this.getClass());

	private final RabbitTemplate template;

	private final AbstractMessageListenerContainer container;

	private final DirectReplyToMessageListenerContainer directReplyToContainer;

	private final String replyAddress;

	@SuppressWarnings("rawtypes")
	private final ConcurrentMap<String, RabbitFuture> pending = new ConcurrentHashMap<String, RabbitFuture>();

	@SuppressWarnings("rawtypes")
	private final CorrelationMessagePostProcessor messagePostProcessor = new CorrelationMessagePostProcessor<>();

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
	public AsyncRabbitTemplate(ConnectionFactory connectionFactory, String exchange, String routingKey,
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
	public AsyncRabbitTemplate(ConnectionFactory connectionFactory, String exchange, String routingKey,
			String replyQueue, String replyAddress) {
		Assert.notNull(connectionFactory, "'connectionFactory' cannot be null");
		Assert.notNull(routingKey, "'routingKey' cannot be null");
		Assert.notNull(replyQueue, "'replyQueue' cannot be null");
		this.template = new RabbitTemplate(connectionFactory);
		this.template.setExchange(exchange == null ? "" : exchange);
		this.template.setRoutingKey(routingKey);
		this.container = new SimpleMessageListenerContainer(connectionFactory);
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
	public AsyncRabbitTemplate(RabbitTemplate template, AbstractMessageListenerContainer container) {
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
	public AsyncRabbitTemplate(RabbitTemplate template, AbstractMessageListenerContainer container,
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
	public AsyncRabbitTemplate(ConnectionFactory connectionFactory, String exchange, String routingKey) {
		Assert.notNull(connectionFactory, "'connectionFactory' cannot be null");
		Assert.notNull(routingKey, "'routingKey' cannot be null");
		this.template = new RabbitTemplate(connectionFactory);
		this.template.setExchange(exchange == null ? "" : exchange);
		this.template.setRoutingKey(routingKey);
		this.container = null;
		this.replyAddress = null;
		this.directReplyToContainer = new DirectReplyToMessageListenerContainer(this.template.getConnectionFactory());
		this.directReplyToContainer.setChannelAwareMessageListener(this);
	}

	/**
	 * Construct an instance using the provided arguments. "Direct replyTo" is used for
	 * replies.
	 * @param template a {@link RabbitTemplate}
	 * @since 2.0
	 */
	public AsyncRabbitTemplate(RabbitTemplate template) {
		Assert.notNull(template, "'template' cannot be null");
		this.template = template;
		this.container = null;
		this.replyAddress = null;
		this.directReplyToContainer = new DirectReplyToMessageListenerContainer(this.template.getConnectionFactory());
		this.directReplyToContainer.setChannelAwareMessageListener(this);
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
		this.template.setReturnCallback(this);
		this.template.setMandatory(mandatory);
	}

	/**
	 * @param mandatoryExpression a SpEL {@link Expression} to evaluate against each request
	 * message. The result of the evaluation must be a {@code boolean} value.
	 * @since 2.0
	 */
	public void setMandatoryExpression(Expression mandatoryExpression) {
		this.template.setReturnCallback(this);
		this.template.setMandatoryExpression(mandatoryExpression);
	}

	/**
	 * @param mandatoryExpression a SpEL {@link Expression} to evaluate against each request
	 * message. The result of the evaluation must be a {@code boolean} value.
	 * @since 2.0
	 */
	public void setMandatoryExpressionString(String mandatoryExpression) {
		this.template.setReturnCallback(this);
		this.template.setMandatoryExpressionString(mandatoryExpression);
	}

	/**
	 * Set to true to enable publisher confirms. When enabled, the {@link RabbitFuture}
	 * returned by the send and receive operation will have a
	 * {@code ListenableFuture<Boolean>} in its {@code confirm} property.
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
		String correlationId = getOrSetCorrelationIdAndSetReplyTo(message);
		RabbitMessageFuture future = new RabbitMessageFuture(correlationId, message);
		CorrelationData correlationData = null;
		if (this.enableConfirms) {
			correlationData = new CorrelationData(correlationId);
			future.setConfirm(new SettableListenableFuture<>());
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
			MessagePostProcessor messagePostProcessor) {
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
			MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<C> responseType) {
		return convertSendAndReceiveAsType(this.template.getExchange(), routingKey, object, messagePostProcessor,
				responseType);
	}

	@Override
	public <C> RabbitConverterFuture<C> convertSendAndReceiveAsType(String exchange, String routingKey, Object object,
			MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<C> responseType) {
		Assert.state(this.template.getMessageConverter() instanceof SmartMessageConverter,
				"template's message converter must be a SmartMessageConverter");
		return convertSendAndReceive(exchange, routingKey, object, messagePostProcessor, responseType);
	}

	private <C> RabbitConverterFuture<C> convertSendAndReceive(String exchange, String routingKey, Object object,
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
			this.messagePostProcessor.postProcessMessage(message, correlationData);
			ChannelHolder channelHolder = this.directReplyToContainer.getChannelHolder();
			correlationData.future.setChannelHolder(channelHolder);
			sendDirect(channelHolder.getChannel(), exchange, routingKey, message, correlationData);
		}
		RabbitConverterFuture<C> future = correlationData.future;
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

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
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
				RabbitFuture<?> future = this.pending.remove(correlationId);
				if (future != null) {
					if (future instanceof AsyncRabbitTemplate.RabbitConverterFuture) {
						MessageConverter messageConverter = this.template.getMessageConverter();
						RabbitConverterFuture<Object> rabbitFuture = (RabbitConverterFuture<Object>) future;
						Object converted = rabbitFuture.getReturnType() != null
								&& messageConverter instanceof SmartMessageConverter
										? ((SmartMessageConverter) messageConverter).fromMessage(message,
												rabbitFuture.getReturnType())
										: messageConverter.fromMessage(message);
						rabbitFuture.set(converted);
					}
					else {
						((RabbitMessageFuture) future).set(message);
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
	public void returnedMessage(Message message, int replyCode, String replyText, String exchange, String routingKey) {
		MessageProperties messageProperties = message.getMessageProperties();
		String correlationId = messageProperties.getCorrelationId();
		if (StringUtils.hasText(correlationId)) {
			RabbitFuture<?> future = this.pending.remove(correlationId);
			if (future != null) {
				future.setException(new AmqpMessageReturnedException("Message returned", message, replyCode, replyText,
						exchange, routingKey));
			}
			else {
				if (this.logger.isWarnEnabled()) {
					this.logger.warn("No pending reply - perhaps timed out? Message returned: " + message);
				}
			}
		}
	}

	@Override
	public void confirm(CorrelationData correlationData, boolean ack, String cause) {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Confirm: " + correlationData + ", ack=" + ack
					+ (cause == null ? "" : (", cause: " + cause)));
		}
		String correlationId = correlationData.getId();
		if (correlationId != null) {
			RabbitFuture<?> future = this.pending.get(correlationId);
			if (future != null) {
				future.setNackCause(cause);
				((SettableListenableFuture<Boolean>) future.getConfirm()).set(ack);
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

	private String getOrSetCorrelationIdAndSetReplyTo(Message message) {
		String correlationId;
		MessageProperties messageProperties = message.getMessageProperties();
		Assert.notNull(messageProperties, "the message properties cannot be null");
		String currentCorrelationId = messageProperties.getCorrelationId();
		if (!StringUtils.hasText(currentCorrelationId)) {
			correlationId = UUID.randomUUID().toString();
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
	 * Base class for {@link ListenableFuture}s returned by {@link AsyncRabbitTemplate}.
	 * @param <T> the type.
	 * @since 1.6
	 */
	public abstract class RabbitFuture<T> extends SettableListenableFuture<T> {

		private final String correlationId;

		private final Message requestMessage;

		private ScheduledFuture<?> timeoutTask;

		private volatile ListenableFuture<Boolean> confirm;

		private String nackCause;

		private ChannelHolder channelHolder;

		public RabbitFuture(String correlationId, Message requestMessage) {
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
			AsyncRabbitTemplate.this.pending.remove(this.correlationId);
			if (this.channelHolder != null && AsyncRabbitTemplate.this.directReplyToContainer != null) {
				AsyncRabbitTemplate.this.directReplyToContainer.releaseConsumerFor(this.channelHolder, false, null);
			}
			return super.cancel(mayInterruptIfRunning);
		}

		/**
		 * When confirms are enabled contains a {@link ListenableFuture}
		 * for the confirmation.
		 * @return the future.
		 */
		public ListenableFuture<Boolean> getConfirm() {
			return this.confirm;
		}

		void setConfirm(ListenableFuture<Boolean> confirm) {
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
			if (AsyncRabbitTemplate.this.receiveTimeout > 0) {
				synchronized (AsyncRabbitTemplate.this) {
					if (!AsyncRabbitTemplate.this.running) {
						AsyncRabbitTemplate.this.pending.remove(this.correlationId);
						throw new IllegalStateException("'AsyncRabbitTemplate' must be started.");
					}
					this.timeoutTask = AsyncRabbitTemplate.this.taskScheduler.schedule(new TimeoutTask(),
							new Date(System.currentTimeMillis() + AsyncRabbitTemplate.this.receiveTimeout));
				}
			}
			else {
				this.timeoutTask = null;
			}
		}

		private class TimeoutTask implements Runnable {

			@Override
			public void run() {
				AsyncRabbitTemplate.this.pending.remove(RabbitFuture.this.correlationId);
				if (RabbitFuture.this.channelHolder != null
						&& AsyncRabbitTemplate.this.directReplyToContainer != null) {
					AsyncRabbitTemplate.this.directReplyToContainer
							.releaseConsumerFor(RabbitFuture.this.channelHolder, false, null);
				}
				setException(new AmqpReplyTimeoutException("Reply timed out", RabbitFuture.this.requestMessage));
			}

		}

	}

	/**
	 * A {@link RabbitFuture} with a return type of {@link Message}.
	 * @since 1.6
	 */
	public class RabbitMessageFuture extends RabbitFuture<Message> implements ListenableFuture<Message> {

		public RabbitMessageFuture(String correlationId, Message requestMessage) {
			super(correlationId, requestMessage);
		}

	}

	/**
	 * A {@link RabbitFuture} with a return type of the template's
	 * generic parameter.
	 * @param <C> the type.
	 * @since 1.6
	 */
	public class RabbitConverterFuture<C> extends RabbitFuture<C> implements ListenableFuture<C> {

		private volatile ParameterizedTypeReference<C> returnType;

		public RabbitConverterFuture(String correlationId, Message requestMessage) {
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
			super();
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
			String correlationId = getOrSetCorrelationIdAndSetReplyTo(messageToSend);
			correlationData.future = new RabbitConverterFuture<C>(correlationId, message);
			if (correlationData.enableConfirms && correlationData.getId() == null) {
				correlationData.setId(correlationId);
				correlationData.future.setConfirm(new SettableListenableFuture<>());
			}
			correlationData.future.setReturnType(correlationData.returnType);
			AsyncRabbitTemplate.this.pending.put(correlationId, correlationData.future);
			return messageToSend;
		}

	}

	private static class AsyncCorrelationData<C> extends CorrelationData {

		private final MessagePostProcessor userPostProcessor;

		private final ParameterizedTypeReference<C> returnType;

		private final boolean enableConfirms;

		private volatile RabbitConverterFuture<C> future;

		AsyncCorrelationData(MessagePostProcessor userPostProcessor, ParameterizedTypeReference<C> returnType,
				boolean enableConfirms) {

			this.userPostProcessor = userPostProcessor;
			this.returnType = returnType;
			this.enableConfirms = enableConfirms;
		}

	}

}
