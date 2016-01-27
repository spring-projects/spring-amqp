/*
 * Copyright 2016 the original author or authors.
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

import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpMessageReturnedException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

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
 * {@link AmqpMessageReturnedException} cause of an {@link ExecutionException}.
 * <p>
 * Internally, the template uses a {@link RabbitTemplate} and a
 * {@link SimpleMessageListenerContainer} either provided or constructed internally.
 * If an external {@link RabbitTemplate} is provided and confirms/returns are enabled,
 * it must not previously have had callbacks registered because this object needs to
 * be the callback.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class AsyncRabbitTemplate<C> implements SmartLifecycle, MessageListener, ReturnCallback, ConfirmCallback {

	private final Log logger = LogFactory.getLog(this.getClass());

	private final RabbitTemplate template;

	private final SimpleMessageListenerContainer container;

	private final String replyAddress;

	@SuppressWarnings("rawtypes")
	private final ConcurrentMap<String, RabbitFuture> pending = new ConcurrentHashMap<String, RabbitFuture>();

	private volatile boolean running;

	private volatile boolean enableConfirms;

	private int phase;

	private boolean autoStartup = true;

	private Charset charset = Charset.forName("UTF-8");

	/**
	 * Construct an instance using the provided arguments. Replies will be
	 * routed to the default exchange using the reply queue name as the routing
	 * key.
	 *
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
	 *
	 * @param connectionFactory the connection factory.
	 * @param exchange the default exchange to which requests will be sent.
	 * @param routingKey the default routing key.
	 * @param replyQueue the name of the reply queue to listen for replies.
	 * @param replyAddress the reply address (exchange/routingKey).
	 */
	public AsyncRabbitTemplate(ConnectionFactory connectionFactory, String exchange, String routingKey,
			String replyQueue, String replyAddress) {
		Assert.notNull(connectionFactory, "'connectionFactory' cannot be null");
		this.template = new RabbitTemplate(connectionFactory);
		Assert.notNull(routingKey, "'exchange' cannot be null, use \"\" for the default exchange");
		this.template.setExchange(exchange == null ? "" : exchange);
		Assert.notNull(routingKey, "'routingKey' cannot be null");
		this.template.setRoutingKey(routingKey);
		this.container = new SimpleMessageListenerContainer(connectionFactory);
		Assert.notNull(replyQueue, "'replyQueue' cannot be null");
		this.container.setQueueNames(replyQueue);
		this.container.setMessageListener(this);
		this.container.afterPropertiesSet();
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
	 *
	 * @param template a {@link RabbitTemplate}
	 * @param container a {@link SimpleMessageListenerContainer}.
	 */
	public AsyncRabbitTemplate(RabbitTemplate template, SimpleMessageListenerContainer container) {
		this(template, container, null);
	}

	/**
	 * Construct an instance using the provided arguments. The first queue the container
	 * is configured to listen to will be used as the reply queue. If 'replyAddress' is
	 * null, replies will be routed using the default exchange with that queue name as the
	 * routing key. Otherwise it should have the form exchange/routingKey and must
	 * cause messages to be routed to the reply queue.
	 *
	 * @param template a {@link RabbitTemplate}.
	 * @param container a {@link SimpleMessageListenerContainer}.
	 * @param replyAddress the reply address.
	 */
	public AsyncRabbitTemplate(RabbitTemplate template, SimpleMessageListenerContainer container, String replyAddress) {
		Assert.notNull(template, "'template' cannot be null");
		this.template = template;
		Assert.notNull(container, "'container' cannot be null");
		this.container = container;
		this.container.setMessageListener(this);
		if (replyAddress == null) {
			this.replyAddress = container.getQueueNames()[0];
		}
		else {
			this.replyAddress = replyAddress;
		}
	}

	/**
	 * @see #isAutoStartup()
	 * @param autoStartup true for auto start.
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * @see #getPhase()
	 * @param phase the phase.
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
		if (mandatory) {
			this.template.setReturnCallback(this);
		}
		this.template.setMandatory(mandatory);
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

	/**
	 * Send a message to the default exchange with the default routing key. If the message
	 * contains a correlationId property, it must be unique.
	 * @param message the message.
	 * @return the {@link RabbitMessageFuture}.
	 */
	public RabbitMessageFuture sendAndReceive(Message message) {
		return sendAndReceive(this.template.getExchange(), this.template.getRoutingKey(), message);
	}

	/**
	 * Set the charset to be used when converting byte[] to/from String for
	 * correlation Ids. Default: UTF-8.
	 * @param charset the charset.
	 */
	public void setCharset(Charset charset) {
		this.charset = charset;
	}

	/**
	 * Send a message to the default exchange with the supplied routing key. If the message
	 * contains a correlationId property, it must be unique.
	 * @param routingKey the routing key.
	 * @param message the message.
	 * @return the {@link RabbitMessageFuture}.
	 */
	public RabbitMessageFuture sendAndReceive(String routingKey, Message message) {
		return sendAndReceive(this.template.getExchange(), routingKey, message);
	}

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * default routing key.
	 * @param message the message.
	 * @return the {@link RabbitConverterFuture}.
	 */
	public RabbitConverterFuture convertSendAndReceive(Object message) {
		return convertSendAndReceive(this.template.getExchange(), this.template.getRoutingKey(), message, null);
	}

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * provided routing key.
	 * @param routingKey the routing key.
	 * @param message the message.
	 * @return the {@link RabbitConverterFuture}.
	 */
	public RabbitConverterFuture convertSendAndReceive(String routingKey, Object message) throws AmqpException {
		return convertSendAndReceive(this.template.getExchange(), routingKey, message, null);
	}

	/**
	 * Convert the object to a message and send it to the provided exchange and
	 * routing key.
	 * @param exchange the exchange.
	 * @param routingKey the routing key.
	 * @param message the message.
	 * @return the {@link RabbitConverterFuture}.
	 */
	public RabbitConverterFuture convertSendAndReceive(String exchange, String routingKey, Object message)
			throws AmqpException {
		return convertSendAndReceive(exchange, routingKey, message, null);
	}

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * default routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param message the message.
	 * @param messagePostProcessor the post processor.
	 * @return the {@link RabbitConverterFuture}.
	 */
	public RabbitConverterFuture convertSendAndReceive(Object message, MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		return convertSendAndReceive(this.template.getExchange(), this.template.getRoutingKey(), message,
				messagePostProcessor);
	}

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * provided routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param routingKey the routing key.
	 * @param message the message.
	 * @param messagePostProcessor the post processor.
	 * @return the {@link RabbitConverterFuture}.
	 */
	public RabbitConverterFuture convertSendAndReceive(String routingKey, Object message,
			MessagePostProcessor messagePostProcessor) throws AmqpException {
		return convertSendAndReceive(this.template.getExchange(), routingKey, message,
				messagePostProcessor);
	}

	/**
	 * Send a message to the supplied exchange and routing key. If the message
	 * contains a correlationId property, it must be unique.
	 * @param exchange the exchange.
	 * @param routingKey the routing key.
	 * @param message the message.
	 * @return the {@link RabbitMessageFuture}.
	 */
	public RabbitMessageFuture sendAndReceive(String exchange, String routingKey, Message message) {
		String correlationId = getOrSetCorrelationIdAndSetReplyTo(message);
		RabbitMessageFuture future = new RabbitMessageFuture(correlationId);
		CorrelationData correlationData = null;
		if (this.enableConfirms) {
			correlationData = new CorrelationData(correlationId);
			future.setConfirm(new SettableListenableFuture<Boolean>());
		}
		this.pending.put(correlationId, future);
		this.template.send(exchange, routingKey, message, correlationData);
		return future;
	}

	/**
	 * Convert the object to a message and send it to the provided exchange and
	 * routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param exchange the exchange
	 * @param routingKey the routing key.
	 * @param message the message.
	 * @param messagePostProcessor the post processor.
	 * @return the {@link RabbitConverterFuture}.
	 */
	public RabbitConverterFuture convertSendAndReceive(String exchange, String routingKey, Object message,
			MessagePostProcessor messagePostProcessor) throws AmqpException {
		CorrelationData correlationData = null;
		if (this.enableConfirms) {
			correlationData = new MutableCorrelationData(null);
		}
		CorrelationMessagePostProcessor correlationPostProcessor = new CorrelationMessagePostProcessor(
				messagePostProcessor, correlationData);
		this.template.convertAndSend(exchange, routingKey, message, correlationPostProcessor, correlationData);
		return correlationPostProcessor.getFuture();
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			this.container.start();
		}
		this.running = true;
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			this.container.stop();
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
	public void onMessage(Message message) {
		MessageProperties messageProperties = message.getMessageProperties();
		if (messageProperties != null) {
			byte[] correlationId = messageProperties.getCorrelationId();
			if (correlationId != null) {
				if (logger.isDebugEnabled()) {
					logger.debug("onMessage: " + message);
				}
				RabbitFuture<?> future = this.pending.remove(new String(correlationId, this.charset));
				if (future != null) {
					if (future instanceof AsyncRabbitTemplate.RabbitConverterFuture) {
						C converted = (C) this.template.getMessageConverter().fromMessage(message);
						((RabbitConverterFuture) future).set(converted);
					}
					else {
						((RabbitMessageFuture) future).set(message);
					}
				}
			}
		}
	}

	@Override
	public void returnedMessage(Message message, int replyCode, String replyText, String exchange, String routingKey) {
		String correlationId = new String(message.getMessageProperties().getCorrelationId(), this.charset);
		if (correlationId != null) {
			@SuppressWarnings("unchecked")
			RabbitFuture<?> future = this.pending.remove(correlationId);
			if (future != null) {
				future.setException(new AmqpMessageReturnedException("Message returned", message, replyCode, replyText,
						exchange, routingKey));
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void confirm(CorrelationData correlationData, boolean ack, String cause) {
		if (logger.isDebugEnabled()) {
			logger.debug("Confirm: " + correlationData + ", ack=" + ack
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
				if (logger.isWarnEnabled()) {
					logger.warn("Confirm: " + correlationData + ", ack=" + ack
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
		byte[] currentCorrelationId = messageProperties.getCorrelationId();
		if (currentCorrelationId == null) {
			correlationId = UUID.randomUUID().toString();
			messageProperties.setCorrelationId(correlationId.getBytes(this.charset));
			Assert.isNull(messageProperties.getReplyTo(), "'replyTo' property must be null");
		}
		else {
			correlationId = new String(currentCorrelationId, this.charset);
		}
		messageProperties.setReplyTo(this.replyAddress);
		return correlationId;
	}

	/**
	 * Base class for {@link ListenableFuture}s returned by {@link AsyncRabbitTemplate}.
	 * @since 1.6
	 *
	 */
	public abstract class RabbitFuture<T> extends SettableListenableFuture<T> {

		private final String correlationId;

		private volatile ListenableFuture<Boolean> confirm;

		private String nackCause;

		public RabbitFuture(String correlationId) {
			this.correlationId = correlationId;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			pending.remove(this.correlationId);
			boolean cancelled = super.cancel(mayInterruptIfRunning);
			return cancelled;
		}

		/**
		 * When confirms are enabled contains a {@link ListenableFuture}
		 * for the confirmation.
		 * @return the future.
		 */
		public ListenableFuture<Boolean> getConfirm() {
			return confirm;
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
			return nackCause;
		}

		void setNackCause(String nackCause) {
			this.nackCause = nackCause;
		}

	}

	/**
	 * A {@link RabbitFuture} with a return type of {@link Message}.
	 *
	 * @since 1.6
	 *
	 */
	public class RabbitMessageFuture extends RabbitFuture<Message> {

		public RabbitMessageFuture(String correlationId) {
			super(correlationId);
		}

	}

	/**
	 * A {@link RabbitFuture} with a return type of the template's
	 * generic parameter.
	 *
	 * @since 1.6
	 *
	 */
	public class RabbitConverterFuture extends RabbitFuture<C> {

		public RabbitConverterFuture(String correlationId) {
			super(correlationId);
		}

	}

	private final class CorrelationMessagePostProcessor implements MessagePostProcessor {

		private final MessagePostProcessor userPostProcessor;

		private final CorrelationData correlationData;

		private volatile RabbitConverterFuture future;

		public CorrelationMessagePostProcessor(MessagePostProcessor userPostProcessor,
				CorrelationData correlationData) {
			this.userPostProcessor = userPostProcessor;
			this.correlationData = correlationData;
		}

		@Override
		public Message postProcessMessage(Message message) throws AmqpException {
			Message messageToSend = message;
			if (this.userPostProcessor != null) {
				messageToSend = this.userPostProcessor.postProcessMessage(message);
			}
			String correlationId = getOrSetCorrelationIdAndSetReplyTo(messageToSend);
			this.future = new RabbitConverterFuture(correlationId);
			if (this.correlationData instanceof MutableCorrelationData) {
				((MutableCorrelationData) correlationData).setId(correlationId);
				future.setConfirm(new SettableListenableFuture<Boolean>());
			}
			AsyncRabbitTemplate.this.pending.put(correlationId, future);
			return messageToSend;
		}

		private RabbitConverterFuture getFuture() {
			return this.future;
		}

	}

	/**
	 * This class exists simply to increase the visibility of the id setter.
	 */
	private static final class MutableCorrelationData extends CorrelationData {

		public MutableCorrelationData(String id) {
			super(id);
		}

		@Override
		protected void setId(String id) {
			super.setId(id);
		}

	}

}
