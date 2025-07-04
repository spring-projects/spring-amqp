/*
 * Copyright 2002-present the original author or authors.
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

import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.MessagingMessageConverter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.core.AbstractMessagingTemplate;
import org.springframework.messaging.core.MessagePostProcessor;
import org.springframework.util.Assert;

/**
 * An implementation of {@link RabbitMessageOperations}.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.4
 */
public class RabbitMessagingTemplate extends AbstractMessagingTemplate<String>
		implements RabbitMessageOperations, InitializingBean {

	@SuppressWarnings("NullAway.Init")
	private RabbitTemplate rabbitTemplate;

	private MessageConverter amqpMessageConverter = new MessagingMessageConverter();

	private boolean converterSet;

	private boolean useTemplateDefaultReceiveQueue;

	/**
	 * Constructor for use with bean properties.
	 * Requires {@link #setRabbitTemplate} to be called.
	 */
	public RabbitMessagingTemplate() {
	}

	/**
	 * Create an instance with the {@link RabbitTemplate} to use.
	 * @param rabbitTemplate the template.
	 */
	public RabbitMessagingTemplate(RabbitTemplate rabbitTemplate) {
		Assert.notNull(rabbitTemplate, "'rabbitTemplate' must not be null");
		this.rabbitTemplate = rabbitTemplate;
	}

	/**
	 * Set the {@link RabbitTemplate} to use.
	 * @param rabbitTemplate the template.
	 */
	public void setRabbitTemplate(RabbitTemplate rabbitTemplate) {
		Assert.notNull(rabbitTemplate, "'rabbitTemplate' must not be null");
		this.rabbitTemplate = rabbitTemplate;
	}

	/**
	 * @return the configured {@link RabbitTemplate}.
	 */
	public RabbitTemplate getRabbitTemplate() {
		return this.rabbitTemplate;
	}

	/**
	 * Set the {@link MessageConverter} to use to convert a {@link Message} from
	 * the messaging to and from a {@link org.springframework.amqp.core.Message}.
	 * By default, a {@link MessagingMessageConverter} is defined using the provided
	 * {@link RabbitTemplate}'s message converter (a
	 * {@link org.springframework.amqp.support.converter.SimpleMessageConverter}
	 * by default) to convert the payload of the message.
	 * <p>Consider configuring a {@link MessagingMessageConverter} with a different
	 * {@link MessagingMessageConverter#setPayloadConverter(MessageConverter) payload converter}
	 * for more advanced scenarios.
	 * @param amqpMessageConverter the message converter.
	 * @see MessagingMessageConverter
	 */
	public void setAmqpMessageConverter(MessageConverter amqpMessageConverter) {
		this.amqpMessageConverter = amqpMessageConverter;
		this.converterSet = true;
	}

	/**
	 * @return the {@link MessageConverter} to use to convert a {@link org.springframework.messaging.Message}
	 * from the messaging to and from a {@link org.springframework.amqp.core.Message}.
	 */
	public MessageConverter getAmqpMessageConverter() {
		return this.amqpMessageConverter;
	}

	/**
	 * When true, use the underlying {@link RabbitTemplate}'s defaultReceiveQueue property
	 * (if configured) for receive only methods instead of the {@code defaultDestination}
	 * configured in this template. Set this to true to use the template's queue instead.
	 * Default false, but will be true in a future release.
	 * @param useTemplateDefaultReceiveQueue true to use the template's queue.
	 * @since 2.2.22
	 */
	public void setUseTemplateDefaultReceiveQueue(boolean useTemplateDefaultReceiveQueue) {
		this.useTemplateDefaultReceiveQueue = useTemplateDefaultReceiveQueue;
	}

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(getRabbitTemplate(), "Property 'rabbitTemplate' is required");
		Assert.notNull(getAmqpMessageConverter(), "Property 'amqpMessageConverter' is required");
		if (!this.converterSet) {
			((MessagingMessageConverter) this.amqpMessageConverter)
					.setPayloadConverter(this.rabbitTemplate.getMessageConverter());
		}
	}

	@Override
	public void send(@Nullable String exchange, @Nullable String routingKey, Message<?> message)
			throws MessagingException {

		doSend(exchange, routingKey, message);
	}

	@Override
	public void convertAndSend(@Nullable String exchange, @Nullable String routingKey, Object payload,
			@Nullable Map<String, Object> headers, @Nullable MessagePostProcessor postProcessor)
			throws MessagingException {

		Message<?> message = doConvert(payload, headers, postProcessor);
		send(exchange, routingKey, message);
	}

	@Override
	public @Nullable Message<?> sendAndReceive(@Nullable String exchange, @Nullable String routingKey,
			Message<?> requestMessage) throws MessagingException {

		return doSendAndReceive(exchange, routingKey, requestMessage);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> @Nullable T convertSendAndReceive(@Nullable String exchange, @Nullable String routingKey, Object request,
			@Nullable Map<String, Object> headers,
			Class<T> targetClass, @Nullable MessagePostProcessor requestPostProcessor) throws MessagingException {

		Message<?> requestMessage = doConvert(request, headers, requestPostProcessor);
		Message<?> replyMessage = sendAndReceive(exchange, routingKey, requestMessage);
		return (replyMessage != null ? (T) getMessageConverter().fromMessage(replyMessage, targetClass) : null);
	}

	@Override
	protected void doSend(@Nullable String destination, Message<?> message) {
		try {
			Object correlation = message.getHeaders().get(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION);
			if (correlation instanceof CorrelationData corrData) {
				this.rabbitTemplate.send(destination, createMessage(message), corrData);
			}
			else {
				this.rabbitTemplate.send(destination, createMessage(message));
			}
		}
		catch (RuntimeException ex) {
			throw convertAmqpException(ex);
		}
	}

	protected void doSend(@Nullable String exchange, @Nullable String routingKey, Message<?> message) {
		try {
			Object correlation = message.getHeaders().get(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION);
			if (correlation instanceof CorrelationData corrData) {
				this.rabbitTemplate.send(exchange, routingKey, createMessage(message), corrData);
			}
			else {
				this.rabbitTemplate.send(exchange, routingKey, createMessage(message));
			}
		}
		catch (RuntimeException ex) {
			throw convertAmqpException(ex);
		}
	}

	@Override
	public @Nullable Message<?> receive() {
		return doReceive(resolveDestination());
	}

	@Override
	public <T> @Nullable T receiveAndConvert(Class<T> targetClass) {
		return receiveAndConvert(resolveDestination(), targetClass);
	}

	private String resolveDestination() {
		String dest = null;
		if (this.useTemplateDefaultReceiveQueue) {
			dest = this.rabbitTemplate.getDefaultReceiveQueue();
		}
		if (dest == null) {
			dest = getRequiredDefaultDestination();
		}
		return dest;
	}

	@Override
	protected @Nullable Message<?> doReceive(String destination) {
		try {
			org.springframework.amqp.core.Message amqpMessage = this.rabbitTemplate.receive(destination);
			return convertAmqpMessage(amqpMessage);
		}
		catch (RuntimeException ex) {
			throw convertAmqpException(ex);
		}
	}

	@Override
	protected @Nullable Message<?> doSendAndReceive(@Nullable String destination, Message<?> requestMessage) {
		try {
			org.springframework.amqp.core.Message amqpMessage = this.rabbitTemplate.sendAndReceive(
					destination, createMessage(requestMessage));
			return convertAmqpMessage(amqpMessage);
		}
		catch (RuntimeException ex) {
			throw convertAmqpException(ex);
		}
	}

	protected @Nullable Message<?> doSendAndReceive(@Nullable String exchange, @Nullable String routingKey,
			Message<?> requestMessage) {
		try {
			org.springframework.amqp.core.Message amqpMessage = this.rabbitTemplate.sendAndReceive(
					exchange, routingKey, createMessage(requestMessage));
			return convertAmqpMessage(amqpMessage);
		}
		catch (RuntimeException ex) {
			throw convertAmqpException(ex);
		}
	}

	private org.springframework.amqp.core.Message createMessage(Message<?> message) {
		try {
			return getAmqpMessageConverter().toMessage(message, new MessageProperties());
		}
		catch (org.springframework.amqp.support.converter.MessageConversionException ex) {
			throw new MessageConversionException("Could not convert '" + message + "'", ex);
		}
	}

	protected @Nullable Message<?> convertAmqpMessage(org.springframework.amqp.core.@Nullable Message message) {
		if (message == null) {
			return null;
		}
		try {
			return (Message<?>) getAmqpMessageConverter().fromMessage(message);
		}
		catch (Exception ex) {
			throw new MessageConversionException("Could not convert '" + message + "'", ex);
		}
	}

	@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
	protected MessagingException convertAmqpException(RuntimeException ex) {
		if (ex instanceof MessagingException mex) {
			return mex;
		}
		if (ex instanceof org.springframework.amqp.support.converter.MessageConversionException) {
			return new MessageConversionException(ex.getMessage(), ex);
		}
		// Fallback
		return new MessagingException(ex.getMessage(), ex);
	}

}
