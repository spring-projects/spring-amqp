/*
 * Copyright 2002-2015 the original author or authors.
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

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.support.AmqpHeaderMapper;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
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
 * @since 1.4
 */
public class MessagingMessageListenerAdapter extends AbstractAdaptableMessageListener {

	private HandlerAdapter handlerMethod;

	private final MessagingMessageConverterAdapter messagingMessageConverter = new MessagingMessageConverterAdapter();


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
		Object result = invokeHandler(amqpMessage, channel, message);
		if (result != null) {
			handleResult(result, amqpMessage, channel);
		}
		else {
			logger.trace("No result object given - no result to handle");
		}
	}

	protected Message<?> toMessagingMessage(org.springframework.amqp.core.Message amqpMessage) {
		return (Message<?>) getMessagingMessageConverter().fromMessage(amqpMessage);
	}

	/**
	 * Invoke the handler, wrapping any exception to a {@link ListenerExecutionFailedException}
	 * with a dedicated error message.
	 */
	private Object invokeHandler(org.springframework.amqp.core.Message amqpMessage, Channel channel,
			Message<?> message) {
		try {
			return this.handlerMethod.invoke(message, amqpMessage, channel);
		}
		catch (org.springframework.messaging.converter.MessageConversionException ex) {
			throw new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
					"be invoked with the incoming message", message.getPayload()),
					new MessageConversionException("Cannot handle message", ex));
		}
		catch (MessagingException ex) {
			throw new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
					"be invoked with the incoming message", message.getPayload()), ex);
		}
		catch (Exception ex) {
			throw new ListenerExecutionFailedException("Listener method '" +
					this.handlerMethod.getMethodAsString(message.getPayload()) + "' threw exception", ex);
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
	 * to enforce backward compatibility.
	 */
	private class MessagingMessageConverterAdapter extends MessagingMessageConverter {

		@Override
		protected Object extractPayload(org.springframework.amqp.core.Message message) {
			return extractMessage(message);
		}

	}

}
