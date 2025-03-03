/*
 * Copyright 2025 the original author or authors.
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

package org.springframework.amqp.rabbitmq.client.listener;

import java.lang.reflect.Method;

import com.rabbitmq.client.amqp.Consumer;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.listener.adapter.InvocationResult;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpUtils;

/**
 * A {@link MessagingMessageListenerAdapter} extension for the {@link RabbitAmqpMessageListener}.
 * Provides these arguments for the {@link #getHandlerAdapter()} invocation:
 * <ul>
 * <li>{@link com.rabbitmq.client.amqp.Message} - the native AMQP 1.0 message without any conversions</li>
 * <li>{@link org.springframework.amqp.core.Message} - Spring AMQP message abstraction as conversion result from the native AMQP 1.0 message</li>
 * <li>{@link org.springframework.messaging.Message} - Spring Messaging abstraction as conversion result from the Spring AMQP message</li>
 * <li>{@link Consumer.Context} - RabbitMQ AMQP client consumer settlement API.</li>
 * <li>{@link org.springframework.amqp.core.AmqpAcknowledgment} - Spring AMQP acknowledgment abstraction: delegates to the {@link Consumer.Context}</li>
 * </ul>
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public class RabbitAmqpMessageListenerAdapter extends MessagingMessageListenerAdapter
		implements RabbitAmqpMessageListener {

	public RabbitAmqpMessageListenerAdapter(@Nullable Object bean, @Nullable Method method, boolean returnExceptions,
			@Nullable RabbitListenerErrorHandler errorHandler) {

		super(bean, method, returnExceptions, errorHandler);
	}

	@Override
	public void onAmqpMessage(com.rabbitmq.client.amqp.Message amqpMessage, Consumer.@Nullable Context context) {
		org.springframework.amqp.core.Message springMessage = RabbitAmqpUtils.fromAmqpMessage(amqpMessage, context);
		try {
			org.springframework.messaging.Message<?> messagingMessage = toMessagingMessage(springMessage);
			InvocationResult result = getHandlerAdapter()
					.invoke(messagingMessage,
							springMessage, springMessage.getMessageProperties().getAmqpAcknowledgment(),
							amqpMessage, context);
			if (result.getReturnValue() != null) {
				logger.warn("Replies are not currently supported with RabbitMQ AMQP 1.0 listeners");
			}
		}
		catch (Exception ex) {
			throw new ListenerExecutionFailedException("Failed to invoke listener", ex, springMessage);
		}
	}

}
