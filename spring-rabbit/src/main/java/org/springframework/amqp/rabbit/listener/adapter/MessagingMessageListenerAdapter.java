/*
 * Copyright 2002-2025 the original author or authors.
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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.rabbitmq.client.Channel;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.support.AmqpHeaderMapper;
import org.springframework.amqp.support.SimpleAmqpHeaderMapper;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.MessagingMessageConverter;
import org.springframework.amqp.support.converter.RemoteInvocationResult;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.Assert;
import org.springframework.util.TypeUtils;

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

	private final MessagingMessageConverterAdapter messagingMessageConverter;

	private final boolean returnExceptions;

	private final @Nullable RabbitListenerErrorHandler errorHandler;

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

	protected MessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method, boolean returnExceptions,
			@Nullable RabbitListenerErrorHandler errorHandler, boolean batch) {

		this.messagingMessageConverter = new MessagingMessageConverterAdapter(bean, method, batch);
		this.returnExceptions = returnExceptions;
		this.errorHandler = errorHandler;
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
			handleException(amqpMessage, channel, message, new ListenerExecutionFailedException(
					"Failed to convert message", ex, amqpMessage));
		}
	}

	@Override
	protected void asyncFailure(org.springframework.amqp.core.Message request, Channel channel, Throwable t,
			@Nullable Object source) {

		try {
			handleException(request, channel, (Message<?>) source,
					new ListenerExecutionFailedException("Async Fail", t, request));
			return;
		}
		catch (Exception ex) {
			// Ignore
		}
		super.asyncFailure(request, channel, t, source);
	}

	private void handleException(org.springframework.amqp.core.Message amqpMessage, @Nullable Channel channel,
			@Nullable Message<?> message, ListenerExecutionFailedException e) throws Exception { // NOSONAR

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
					logger.trace("Error handler returned no result");
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
		if (this.messagingMessageConverter.method == null) {
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
			@Nullable Message<?> message, @Nullable Throwable throwableToReturn, Exception exceptionToThrow) throws Exception {

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
			throw new ListenerExecutionFailedException(createMessagingErrorMessage(message.getPayload()),
					ex, amqpMessages);
		}
		catch (Exception ex) {
			throw new ListenerExecutionFailedException("Listener method '" +
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
	 * Build a Rabbit message to be sent as response based on the given result object.
	 * @param channel the Rabbit Channel to operate on
	 * @param result the content of the message, as returned from the listener method
	 * @param genericType the generic type of the result.
	 * @return the Rabbit <code>Message</code> (never <code>null</code>)
	 * @see #setMessageConverter
	 */
	@Override
	protected org.springframework.amqp.core.Message buildMessage(Channel channel, @Nullable Object result,
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

	/**
	 * Delegates payload extraction to
	 * {@link #extractMessage(org.springframework.amqp.core.Message message)}
	 * to enforce backward compatibility. Uses this listener adapter's converter instead of
	 * the one configured in the converter adapter.
	 * If the inbound message has no type information and the configured message converter
	 * supports it, we attempt to infer the conversion type from the method signature.
	 */
	protected final class MessagingMessageConverterAdapter extends MessagingMessageConverter {

		private final @Nullable Object bean;

		final @Nullable Method method; // NOSONAR visibility

		private final @Nullable Type inferredArgumentType;

		private final boolean isBatch;

		private boolean isMessageList;

		private boolean isAmqpMessageList;

		private boolean isCollection;

		MessagingMessageConverterAdapter(@Nullable Object bean, @Nullable Method method, boolean batch) {
			this.bean = bean;
			this.method = method;
			this.isBatch = batch;
			this.inferredArgumentType = determineInferredType();
			if (logger.isDebugEnabled() && this.inferredArgumentType != null) {
				logger.debug("Inferred argument type for " + method + " is " + this.inferredArgumentType);
			}
		}

		protected boolean isMessageList() {
			return this.isMessageList;
		}

		protected boolean isAmqpMessageList() {
			return this.isAmqpMessageList;
		}

		protected @Nullable Method getMethod() {
			return this.method;
		}

		@Override
		protected Object extractPayload(org.springframework.amqp.core.Message message) {
			MessageProperties messageProperties = message.getMessageProperties();
			if (this.bean != null) {
				messageProperties.setTargetBean(this.bean);
			}
			if (this.method != null) {
				messageProperties.setTargetMethod(this.method);
				if (this.inferredArgumentType != null) {
					messageProperties.setInferredArgumentType(this.inferredArgumentType);
				}
			}
			return extractMessage(message);
		}

		private @Nullable Type determineInferredType() { // NOSONAR - complexity
			if (this.method == null) {
				return null;
			}

			Type genericParameterType = null;

			for (int i = 0; i < this.method.getParameterCount(); i++) {
				MethodParameter methodParameter = new MethodParameter(this.method, i);
				/*
				 * We're looking for a single parameter, or one annotated with @Payload.
				 * We ignore parameters with type Message because they are not involved with conversion.
				 */
				boolean isHeaderOrHeaders = methodParameter.hasParameterAnnotation(Header.class)
						|| methodParameter.hasParameterAnnotation(Headers.class)
						|| methodParameter.getParameterType().equals(MessageHeaders.class);
				boolean isPayload = methodParameter.hasParameterAnnotation(Payload.class);
				if (isHeaderOrHeaders && isPayload && MessagingMessageListenerAdapter.this.logger.isWarnEnabled()) {
					MessagingMessageListenerAdapter.this.logger.warn(this.method.getName()
							+ ": Cannot annotate a parameter with both @Header and @Payload; "
							+ "ignored for payload conversion");
				}
				if (isEligibleParameter(methodParameter) && !isHeaderOrHeaders) {

					if (genericParameterType == null) {
						genericParameterType = extractGenericParameterTypFromMethodParameter(methodParameter);
						if (this.isBatch && !this.isCollection) {
							throw new IllegalStateException(
									"Mis-configuration; a batch listener must consume a List<?> or "
											+ "Collection<?> for method: " + this.method);
						}

					}
					else {
						if (MessagingMessageListenerAdapter.this.logger.isDebugEnabled()) {
							MessagingMessageListenerAdapter.this.logger
									.debug("Ambiguous parameters for target payload for method " + this.method
											+ "; no inferred type header added");
						}
						return null;
					}
				}
			}
			return checkOptional(genericParameterType);
		}

		protected @Nullable Type checkOptional(@Nullable Type genericParameterType) {
			if (genericParameterType instanceof ParameterizedType pType && pType.getRawType().equals(Optional.class)) {
				return pType.getActualTypeArguments()[0];
			}
			return genericParameterType;
		}

		/*
		 * Don't consider parameter types that are available after conversion.
		 * Message, Message<?> and Channel.
		 */
		private boolean isEligibleParameter(MethodParameter methodParameter) {
			Type parameterType = methodParameter.getGenericParameterType();
			if (parameterType.equals(Channel.class)
					|| parameterType.equals(MessageProperties.class)
					|| parameterType.equals(org.springframework.amqp.core.Message.class)
					|| parameterType.getTypeName().startsWith("kotlin.coroutines.Continuation")) {
				return false;
			}
			if (parameterType instanceof ParameterizedType parameterizedType &&
					(parameterizedType.getRawType().equals(Message.class))) {
				return !(parameterizedType.getActualTypeArguments()[0] instanceof WildcardType);
			}
			return !parameterType.equals(Message.class); // could be Message without a generic type
		}

		private Type extractGenericParameterTypFromMethodParameter(MethodParameter methodParameter) {
			Type genericParameterType = methodParameter.getGenericParameterType();
			if (genericParameterType instanceof ParameterizedType parameterizedType) {
				if (parameterizedType.getRawType().equals(Message.class)) {
					genericParameterType = ((ParameterizedType) genericParameterType).getActualTypeArguments()[0];
				}
				else if (this.isBatch &&
						(parameterizedType.getRawType().equals(List.class) ||
								(parameterizedType.getRawType().equals(Collection.class) &&
										parameterizedType.getActualTypeArguments().length == 1))) {

					this.isCollection = true;
					Type paramType = parameterizedType.getActualTypeArguments()[0];
					boolean messageHasGeneric = paramType instanceof ParameterizedType pType
							&& pType.getRawType().equals(Message.class);
					this.isMessageList = TypeUtils.isAssignable(paramType, Message.class) || messageHasGeneric;
					this.isAmqpMessageList =
							TypeUtils.isAssignable(paramType, org.springframework.amqp.core.Message.class);
					if (messageHasGeneric) {
						genericParameterType = ((ParameterizedType) paramType).getActualTypeArguments()[0];
					}
					else {
						// when decoding batch messages we convert to the List's generic type
						genericParameterType = paramType;
					}
				}
			}
			return genericParameterType;
		}

	}

}
