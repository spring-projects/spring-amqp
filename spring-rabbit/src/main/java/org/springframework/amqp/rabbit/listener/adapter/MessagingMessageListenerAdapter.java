/*
 * Copyright 2002-2018 the original author or authors.
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

package org.springframework.amqp.rabbit.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.support.AmqpHeaderMapper;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.MessagingMessageConverter;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.remoting.support.RemoteInvocationResult;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;

/**
 * A {@link org.springframework.amqp.core.MessageListener MessageListener}
 * adapter that invokes a configurable {@link HandlerAdapter}.
 *
 * <p>Wraps the incoming {@link org.springframework.amqp.core.Message
 * AMQP Message} to Spring's {@link Message} abstraction, copying the
 * standard headers using a configurable
 * {@link org.springframework.amqp.support.AmqpHeaderMapper AmqpHeaderMapper}.
 *
 * <p>The original {@link org.springframework.amqp.core.Message Message} and
 * the {@link Channel} are provided as additional arguments so that these can
 * be injected as method arguments if necessary.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.4
 */
public class MessagingMessageListenerAdapter extends AbstractAdaptableMessageListener {

	private HandlerAdapter handlerMethod;

	private final MessagingMessageConverterAdapter messagingMessageConverter;

	private final boolean returnExceptions;

	private final RabbitListenerErrorHandler errorHandler;

	public MessagingMessageListenerAdapter() {
		this(null, null);
	}

	public MessagingMessageListenerAdapter(Object bean, Method method) {
		this(bean, method, false, null);
	}

	public MessagingMessageListenerAdapter(Object bean, Method method, boolean returnExceptions,
			RabbitListenerErrorHandler errorHandler) {
		this.messagingMessageConverter = new MessagingMessageConverterAdapter(bean, method);
		this.returnExceptions = returnExceptions;
		this.errorHandler = errorHandler;
	}

	/**
	 * Set the {@link HandlerAdapter} to use to invoke the method
	 * processing an incoming {@link org.springframework.amqp.core.Message}.
	 * @param handlerMethod {@link HandlerAdapter} instance.
	 */
	public void setHandlerMethod(HandlerAdapter handlerMethod) {
		this.handlerMethod = handlerMethod;
	}

	/**
	 * Set the {@link AmqpHeaderMapper} implementation to use to map the standard
	 * AMQP headers. By default, a {@link org.springframework.amqp.support.SimpleAmqpHeaderMapper
	 * SimpleAmqpHeaderMapper} is used.
	 * @param headerMapper the {@link AmqpHeaderMapper} instance.
	 * @see org.springframework.amqp.support.SimpleAmqpHeaderMapper
	 */
	public void setHeaderMapper(AmqpHeaderMapper headerMapper) {
		Assert.notNull(headerMapper, "HeaderMapper must not be null");
		this.messagingMessageConverter.setHeaderMapper(headerMapper);
	}

	/**
	 * @return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 */
	protected final MessagingMessageConverter getMessagingMessageConverter() {
		return this.messagingMessageConverter;
	}

	@Override
	public void onMessage(org.springframework.amqp.core.Message amqpMessage, Channel channel) throws Exception {
		Message<?> message = toMessagingMessage(amqpMessage);
		if (logger.isDebugEnabled()) {
			logger.debug("Processing [" + message + "]");
		}
		try {
			Object result = invokeHandler(amqpMessage, channel, message);
			if (result != null) {
				handleResult(result, amqpMessage, channel, message);
			}
			else {
				logger.trace("No result object given - no result to handle");
			}
		}
		catch (ListenerExecutionFailedException e) {
			if (this.errorHandler != null) {
				try {
					Object result = this.errorHandler.handleError(amqpMessage, message, e);
					if (result != null) {
						handleResult(result, amqpMessage, channel, message);
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
	}

	private void returnOrThrow(org.springframework.amqp.core.Message amqpMessage, Channel channel, Message<?> message,
			Throwable throwableToReturn, Exception exceptionToThrow) throws Exception {
		if (!this.returnExceptions) {
			throw exceptionToThrow;
		}
		try {
			handleResult(new RemoteInvocationResult(throwableToReturn), amqpMessage, channel, message);
		}
		catch (ReplyFailureException rfe) {
			if (void.class.equals(this.handlerMethod.getReturnType(message.getPayload()))) {
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
	 * @param amqpMessage the raw message.
	 * @param channel the channel.
	 * @param message the messaging message.
	 * @return the result of invoking the handler.
	 */
	private Object invokeHandler(org.springframework.amqp.core.Message amqpMessage, Channel channel,
			Message<?> message) {
		try {
			return this.handlerMethod.invoke(message, amqpMessage, channel);
		}
		catch (MessagingException ex) {
			throw new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
					"be invoked with the incoming message", message.getPayload()), ex, amqpMessage);
		}
		catch (Exception ex) {
			throw new ListenerExecutionFailedException("Listener method '" +
					this.handlerMethod.getMethodAsString(message.getPayload()) + "' threw exception", ex, amqpMessage);
		}
	}

	private String createMessagingErrorMessage(String description, Object payload) {
		return description + "\n"
				+ "Endpoint handler details:\n"
				+ "Method [" + this.handlerMethod.getMethodAsString(payload) + "]\n"
				+ "Bean [" + this.handlerMethod.getBean() + "]";
	}

	/**
	 * Build a Rabbit message to be sent as response based on the given result object.
	 * @param channel the Rabbit Channel to operate on
	 * @param result the content of the message, as returned from the listener method
	 * @return the Rabbit <code>Message</code> (never <code>null</code>)
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #setMessageConverter
	 */
	@Override
	protected org.springframework.amqp.core.Message buildMessage(Channel channel, Object result) throws Exception {
		MessageConverter converter = getMessageConverter();
		if (converter != null && !(result instanceof org.springframework.amqp.core.Message)) {
			if (result instanceof org.springframework.messaging.Message) {
				return this.messagingMessageConverter.toMessage(result, new MessageProperties());
			}
			else {
				return converter.toMessage(result, new MessageProperties());
			}
		}
		else {
			if (!(result instanceof org.springframework.amqp.core.Message)) {
				throw new MessageConversionException("No MessageConverter specified - cannot handle message ["
						+ result + "]");
			}
			return (org.springframework.amqp.core.Message) result;
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
	private final class MessagingMessageConverterAdapter extends MessagingMessageConverter {

		private final Object bean;

		private final Method method;

		private final Type inferredArgumentType;

		MessagingMessageConverterAdapter(Object bean, Method method) {
			this.bean = bean;
			this.method = method;
			this.inferredArgumentType = determineInferredType();
			if (logger.isDebugEnabled() && this.inferredArgumentType != null) {
				logger.debug("Inferred argument type for " + method.toString() + " is " + this.inferredArgumentType);
			}
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

		private Type determineInferredType() {
			if (this.method == null) {
				return null;
			}

			Type genericParameterType = null;

			for (int i = 0; i < this.method.getParameterCount(); i++) {
				MethodParameter methodParameter = new MethodParameter(this.method, i);
				/*
				 * We're looking for a single non-annotated parameter, or one annotated with @Payload.
				 * We ignore parameters with type Message because they are not involved with conversion.
				 */
				if (isEligibleParameter(methodParameter)
						&& (methodParameter.getParameterAnnotations().length == 0
						|| methodParameter.hasParameterAnnotation(Payload.class))) {
					if (genericParameterType == null) {
						genericParameterType = methodParameter.getGenericParameterType();
						if (genericParameterType instanceof ParameterizedType) {
							ParameterizedType parameterizedType = (ParameterizedType) genericParameterType;
							if (parameterizedType.getRawType().equals(Message.class)) {
								genericParameterType = ((ParameterizedType) genericParameterType)
									.getActualTypeArguments()[0];
							}
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

			return genericParameterType;
		}

		/*
		 * Don't consider parameter types that are available after conversion.
		 * Message, Message<?> and Channel.
		 */
		private boolean isEligibleParameter(MethodParameter methodParameter) {
			Type parameterType = methodParameter.getGenericParameterType();
			if (parameterType.equals(Channel.class)
					|| parameterType.equals(org.springframework.amqp.core.Message.class)) {
				return false;
			}
			if (parameterType instanceof ParameterizedType) {
				ParameterizedType parameterizedType = (ParameterizedType) parameterType;
				if (parameterizedType.getRawType().equals(Message.class)) {
					return !(parameterizedType.getActualTypeArguments()[0] instanceof WildcardType);
				}
			}
			return !parameterType.equals(Message.class); // could be Message without a generic type
		}

	}

}
