/*
 * Copyright 2002-2014 the original author or authors.
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

import com.rabbitmq.client.Channel;

import org.springframework.amqp.rabbit.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * A {@link org.springframework.amqp.core.MessageListener MessageListener}
 * adapter that invokes a configurable {@link InvocableHandlerMethod}.
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
 * @since 2.0
 */
public class MessagingMessageListenerAdapter extends AbstractAdaptableMessageListener {

	private InvocableHandlerMethod handlerMethod;


	/**
	 * Set the {@link InvocableHandlerMethod} to use to invoke the method
	 * processing an incoming {@link org.springframework.amqp.core.Message}.
	 */
	public void setHandlerMethod(InvocableHandlerMethod handlerMethod) {
		this.handlerMethod = handlerMethod;
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

	@SuppressWarnings("unchecked")
	protected Message<?> toMessagingMessage(org.springframework.amqp.core.Message amqpMessage) {
		return (Message<?>) getMessagingMessageConverter().fromMessage(amqpMessage);
	}

	/**
	 * Invoke the handler, wrapping any exception to a {@link ListenerExecutionFailedException}
	 * with a dedicated error message.
	 */
	private Object invokeHandler(org.springframework.amqp.core.Message amqpMessage, Channel channel, Message<?> message) {
		try {
			return this.handlerMethod.invoke(message, amqpMessage, channel);
		}
		catch (MessagingException ex) {
			throw new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
					"be invoked with the incoming message"), ex);
		}
		catch (Exception ex) {
			throw new ListenerExecutionFailedException("Listener method '" +
					this.handlerMethod.getMethod().toGenericString() + "' threw exception", ex);
		}
	}

	private String createMessagingErrorMessage(String description) {
		StringBuilder sb = new StringBuilder(description).append("\n")
				.append("Endpoint handler details:\n")
				.append("Method [").append(this.handlerMethod.getMethod()).append("]\n")
				.append("Bean [").append(this.handlerMethod.getBean()).append("]\n");
		return sb.toString();
	}

}
