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

package org.springframework.amqp.rabbit.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

import com.rabbitmq.client.Channel;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.listener.ListenerExecutionFailedException;
import org.springframework.amqp.listener.adapter.HandlerAdapter;
import org.springframework.amqp.listener.adapter.InvocationResult;
import org.springframework.amqp.listener.adapter.ReplyFailureException;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.support.AmqpHeaderMapper;
import org.springframework.amqp.support.SimpleAmqpHeaderMapper;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.MessagingMessageConverter;
import org.springframework.amqp.support.converter.MessagingMessageConverterAdapter;
import org.springframework.amqp.support.converter.RemoteInvocationResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.util.Assert;

/**
 * A {@link MessageListener MessageListener}
 * adapter that invokes a configurable {@link HandlerAdapter}.
 *
 * <p>Wraps the incoming {@link org.springframework.amqp.core.Message
 * AMQP Message} to Spring's {@link Message} abstraction, copying the
 * standard headers using a configurable
 * {@link AmqpHeaderMapper AmqpHeaderMapper}.
 *
 * <p>The original {@link org.springframework.amqp.core.Message Message} and
 * the {@link Channel} are provided as additional arguments so that these can
 * be injected as method arguments if necessary.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Kai Stapel
 *
 * @since 1.4
 */
public class MessagingMessageListenerAdapter extends AbstractAdaptableMessageListener {

	protected final MessagingMessageConverterAdapter messagingMessageConverter;

	protected final boolean returnExceptions;

	protected final @Nullable RabbitListenerErrorHandler errorHandler;

	private @Nullable HandlerAdapter handlerAdapter;

	public MessagingMessageListenerAdapter() {
		this(null, null);
	}

	public MessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method) {
		this(bean, method, false, null);
	}

	public MessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method, boolean returnExceptions,
			@Nullable RabbitListenerErrorHandler errorHandler) {

		this(bean, method, returnExceptions, errorHandler, false);
	}

	@SuppressWarnings("this-escape")
	protected MessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method, boolean returnExceptions,
			@Nullable RabbitListenerErrorHandler errorHandler, boolean batch) {

		this.messagingMessageConverter = newMessagingMessageConverterAdapter(bean, method, batch);
		this.returnExceptions = returnExceptions;
		this.errorHandler = errorHandler;
	}

	/**
	 * Create a new {@link MessagingMessageConverterAdapter} instance based on the given bean and method.
	 * By default, additional provided argument type is a {@link Channel}.
	 * Extensions of this class can override this method to provide some other eligible additional argument types
	 * not required conversion.
	 * @param bean the bean of the method.
	 * @param method the method to infer a payload type.
	 * @param batch if the method is a batch listener.
	 * @return the {@link MessagingMessageConverterAdapter} instance.
	 */
	protected MessagingMessageConverterAdapter newMessagingMessageConverterAdapter(
			@Nullable Object bean, @Nullable Method method, boolean batch) {

		return new MessagingMessageConverterAdapter(bean, method, batch, Channel.class);
	}
	/**
	 * Set the {@link HandlerAdapter} to use to invoke the method
	 * processing an incoming {@link org.springframework.amqp.core.Message}.
	 * @param handlerAdapter {@link HandlerAdapter} instance.
	 */
	public void setHandlerAdapter(HandlerAdapter handlerAdapter) {
		this.handlerAdapter = handlerAdapter;
	}

	protected HandlerAdapter getHandlerAdapter() {
		Assert.notNull(this.handlerAdapter, "The 'handlerAdapter' is required");
		return this.handlerAdapter;
	}

	@Override
	public boolean isAsyncReplies() {
		Assert.notNull(this.handlerAdapter, "The 'handlerAdapter' is required");
		return this.handlerAdapter.isAsyncReplies();
	}

	/**
	 * Set the {@link AmqpHeaderMapper} implementation to use to map the standard
	 * AMQP headers. By default, a {@link SimpleAmqpHeaderMapper
	 * SimpleAmqpHeaderMapper} is used.
	 * @param headerMapper the {@link AmqpHeaderMapper} instance.
	 * @see SimpleAmqpHeaderMapper
	 */
	public void setHeaderMapper(AmqpHeaderMapper headerMapper) {
		Assert.notNull(headerMapper, "HeaderMapper must not be null");
		this.messagingMessageConverter.setHeaderMapper(headerMapper);
	}

	/**
	 * @return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link Message}.
	 */
	protected final MessagingMessageConverter getMessagingMessageConverter() {
		return this.messagingMessageConverter;
	}

	@Override
	public void setMessageConverter(@Nullable MessageConverter messageConverter) {
		super.setMessageConverter(messageConverter);
		if (messageConverter != null) {
			this.messagingMessageConverter.setPayloadConverter(messageConverter);
		}
	}

	@Override
	@SuppressWarnings("removal")
	public void onMessage(org.springframework.amqp.core.Message amqpMessage, @Nullable Channel channel)
			throws Exception {

		Message<?> message = null;
		try {
			message = toMessagingMessage(amqpMessage);
			invokeHandlerAndProcessResult(amqpMessage, channel, message);
		}
		catch (ListenerExecutionFailedException ex) {
			handleException(amqpMessage, channel, message, ex);
		}
		catch (ReplyFailureException ex) { // NOSONAR
			throw ex;
		}
		catch (Exception ex) { // NOSONAR
			handleException(amqpMessage, channel, message,
					new org.springframework.amqp.rabbit.support.ListenerExecutionFailedException(
							"Failed to convert message", ex, amqpMessage));
		}
	}

	@Override
	@SuppressWarnings("removal")
	protected void asyncFailure(org.springframework.amqp.core.Message request, @Nullable Channel channel, Throwable t,
			@Nullable Object source) {

		Throwable throwableToHandle = t;

		try {
			handleException(request, channel, (Message<?>) source,
					new org.springframework.amqp.rabbit.support.ListenerExecutionFailedException(
							"Async Fail", throwableToHandle, request));
			return;
		}
		catch (Exception ex) {
			throwableToHandle = ex;
		}

		super.asyncFailure(request, channel, throwableToHandle, source);
	}

	protected void handleException(org.springframework.amqp.core.Message amqpMessage, @Nullable Channel channel,
			@Nullable Message<?> message, ListenerExecutionFailedException e) throws Exception {

		if (this.errorHandler != null) {
			try {
				Object errorResult = this.errorHandler.handleError(amqpMessage, channel, message, e);
				if (errorResult != null) {
					Object payload = message == null ? null : message.getPayload();
					InvocationResult invResult = payload == null
							? new InvocationResult(errorResult, null, null, null, null)
							: getHandlerAdapter().getInvocationResultFor(errorResult, payload);
					handleResult(invResult, amqpMessage, channel, message);
				}
				else {
					logger.trace("Error handler returned no result; acknowledging the message.");
					if (isManualAck()) {
						basicAck(amqpMessage, channel);
					}
				}
			}
			catch (Exception ex) {
				returnOrThrow(amqpMessage, channel, message, ex, ex);
			}
		}
		else {
			returnOrThrow(amqpMessage, channel, message, e.getCause(), e);
		}
	}

	protected void invokeHandlerAndProcessResult(org.springframework.amqp.core.Message amqpMessage,
			@Nullable Channel channel, Message<?> message) {

		boolean projectionUsed = amqpMessage.getMessageProperties().isProjectionUsed();
		if (projectionUsed) {
			amqpMessage.getMessageProperties().setProjectionUsed(false);
		}
		if (logger.isDebugEnabled() && !projectionUsed) {
			logger.debug("Processing [" + message + "]");
		}
		InvocationResult result;
		if (this.messagingMessageConverter.getMethod() == null) {
			amqpMessage.getMessageProperties()
					.setTargetMethod(getHandlerAdapter().getMethodFor(message.getPayload()));
		}
		result = invokeHandler(channel, message, false, amqpMessage);
		if (result.getReturnValue() != null) {
			handleResult(result, amqpMessage, channel, message);
		}
		else {
			logger.trace("No result object given - no result to handle");
		}
	}

	private void returnOrThrow(org.springframework.amqp.core.Message amqpMessage, @Nullable Channel channel,
			@Nullable Message<?> message, @Nullable Throwable throwableToReturn, Exception exceptionToThrow)
			throws Exception {

		if (!this.returnExceptions) {
			throw exceptionToThrow;
		}
		Object payload = message == null ? null : message.getPayload();
		try {
			handleResult(new InvocationResult(new RemoteInvocationResult(throwableToReturn), null,
							payload == null ? Object.class : getHandlerAdapter().getReturnTypeFor(payload),
							getHandlerAdapter().getBean(),
							payload == null ? null : getHandlerAdapter().getMethodFor(payload)),
					amqpMessage, channel, message);
		}
		catch (ReplyFailureException rfe) {
			if (payload == null || void.class.equals(getHandlerAdapter().getReturnTypeFor(payload))) {
				throw exceptionToThrow;
			}
			else {
				throw rfe;
			}
		}
	}

	protected Message<?> toMessagingMessage(org.springframework.amqp.core.Message amqpMessage) {
		return (Message<?>) getMessagingMessageConverter().fromMessage(amqpMessage);
	}

	/**
	 * Invoke the handler, wrapping any exception to a {@link ListenerExecutionFailedException}
	 * with a dedicated error message.
	 * @param amqpMessages the raw AMQP messages.
	 * @param channel the channel.
	 * @param message the messaging message.
	 * @return the result of invoking the handler.
	 */
	@SuppressWarnings("removal")
	protected InvocationResult invokeHandler(@Nullable Channel channel, Message<?> message,
			boolean batch, org.springframework.amqp.core.Message... amqpMessages) {

		try {
			if (batch) {
				return getHandlerAdapter().invoke(message, channel);
			}
			else {
				org.springframework.amqp.core.Message amqpMessage = amqpMessages[0];
				return getHandlerAdapter().invoke(message, amqpMessage, channel, amqpMessage.getMessageProperties());
			}
		}
		catch (MessagingException ex) {
			throw new org.springframework.amqp.rabbit.support.ListenerExecutionFailedException(
					createMessagingErrorMessage(message.getPayload()), ex, amqpMessages);
		}
		catch (Exception ex) {
			throw new org.springframework.amqp.rabbit.support.ListenerExecutionFailedException("Listener method '" +
					getHandlerAdapter().getMethodAsString(message.getPayload()) + "' threw exception", ex, amqpMessages);
		}
	}

	private String createMessagingErrorMessage(Object payload) {
		return "Listener method could not be invoked with the incoming message" + "\n"
				+ "Endpoint handler details:\n"
				+ "Method [" + getHandlerAdapter().getMethodAsString(payload) + "]\n"
				+ "Bean [" + getHandlerAdapter().getBean() + "]";
	}

	/**
	 * Build a Rabbit message to be sent as a response based on the given result object.
	 * @param channel the Rabbit Channel to operate on
	 * @param result the content of the message, as returned from the listener method
	 * @param genericType the generic type of the result.
	 * @return the Rabbit <code>Message</code> (never <code>null</code>)
	 * @see #setMessageConverter
	 */
	@Override
	protected org.springframework.amqp.core.Message buildMessage(@Nullable Channel channel, @Nullable Object result,
			@Nullable Type genericType) {

		MessageConverter converter = getMessageConverter();
		if (converter != null && !(result instanceof org.springframework.amqp.core.Message)) {
			if (result instanceof org.springframework.messaging.Message) {
				return this.messagingMessageConverter.toMessage(result, new MessageProperties());
			}
			else {
				return convert(result, genericType, converter);
			}
		}
		else {
			if (result instanceof org.springframework.amqp.core.Message msg) {
				return msg;
			}
			else {
				throw new MessageConversionException("No MessageConverter specified - cannot handle message ["
						+ result + "]");
			}
		}
	}

}
