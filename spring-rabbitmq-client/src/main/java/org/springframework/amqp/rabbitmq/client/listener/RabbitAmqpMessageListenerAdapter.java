/*
 * Copyright 2025-present the original author or authors.
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.amqp.Consumer;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpAcknowledgment;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.listener.adapter.InvocationResult;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbit.listener.support.ContainerUtils;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpUtils;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

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
 * <p>
 * This class reuses the {@link MessagingMessageListenerAdapter} as much as possible just to avoid duplication.
 * The {@link Channel} abstraction from AMQP Client 0.9.1 is out use and present here just for API compatibility
 * and to follow DRY principle.
 * Can be reworked eventually, when this AMQP 1.0 client won't be based on {@code spring-rabbit} dependency.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public class RabbitAmqpMessageListenerAdapter extends MessagingMessageListenerAdapter
		implements RabbitAmqpMessageListener {

	private @Nullable Collection<MessagePostProcessor> afterReceivePostProcessors;

	private @Nullable RabbitAmqpTemplate rabbitAmqpTemplate;

	public RabbitAmqpMessageListenerAdapter(@Nullable Object bean, @Nullable Method method, boolean returnExceptions,
			@Nullable RabbitListenerErrorHandler errorHandler, boolean batch) {

		super(bean, method, returnExceptions, errorHandler, batch);
	}

	public void setAfterReceivePostProcessors(Collection<MessagePostProcessor> afterReceivePostProcessors) {
		this.afterReceivePostProcessors = new ArrayList<>(afterReceivePostProcessors);
	}

	/**
	 * Set a {@link AmqpConnectionFactory} for publishing replies from this adapter.
	 * @param connectionFactory the {@link AmqpConnectionFactory} for replies.
	 */
	public void setConnectionFactory(AmqpConnectionFactory connectionFactory) {
		this.rabbitAmqpTemplate = new RabbitAmqpTemplate(connectionFactory);
	}

	@Override
	public void onAmqpMessage(com.rabbitmq.client.amqp.Message amqpMessage, Consumer.@Nullable Context context) {
		org.springframework.amqp.core.Message springMessage = RabbitAmqpUtils.fromAmqpMessage(amqpMessage, context);
		if (this.afterReceivePostProcessors != null) {
			for (MessagePostProcessor processor : this.afterReceivePostProcessors) {
				springMessage = processor.postProcessMessage(springMessage);
			}
		}
		try {
			org.springframework.messaging.Message<?> messagingMessage = toMessagingMessage(springMessage);
			InvocationResult result = getHandlerAdapter()
					.invoke(messagingMessage,
							springMessage, springMessage.getMessageProperties().getAmqpAcknowledgment(),
							amqpMessage, context);

			if (result.getReturnValue() != null) {
				Assert.notNull(this.rabbitAmqpTemplate,
						"The 'connectionFactory' must be provided for handling replies.");
				handleResult(result, springMessage, null, messagingMessage);
			}

		}
		catch (Exception ex) {
			throw new ListenerExecutionFailedException("Failed to invoke listener", ex, springMessage);
		}
	}

	@Override
	protected void asyncFailure(Message request, @Nullable Channel channel, Throwable t, @Nullable Object source) {
		try {
			handleException(request, channel, (org.springframework.messaging.Message<?>) source,
					new ListenerExecutionFailedException("Async Fail", t, request));
			return;
		}
		catch (Exception ex) {
			// Ignore and reject the message against original error
		}

		this.logger.error("Future, Mono, or suspend function was completed with an exception for " + request, t);
		AmqpAcknowledgment amqpAcknowledgment = request.getMessageProperties().getAmqpAcknowledgment();
		Assert.notNull(amqpAcknowledgment, "'(amqpAcknowledgment' must be provided into request message.");

		if (ContainerUtils.shouldRequeue(isDefaultRequeueRejected(), t, this.logger)) {
			amqpAcknowledgment.acknowledge(AmqpAcknowledgment.Status.REQUEUE);
		}
		else {
			amqpAcknowledgment.acknowledge(AmqpAcknowledgment.Status.REJECT);
		}
	}

	@Override
	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	protected void sendResponse(@Nullable Channel channel, Address replyTo, Message messageIn) {
		Message replyMessage = messageIn;
		MessagePostProcessor[] beforeSendReplyPostProcessors = getBeforeSendReplyPostProcessors();
		if (beforeSendReplyPostProcessors != null) {
			for (MessagePostProcessor postProcessor : beforeSendReplyPostProcessors) {
				replyMessage = postProcessor.postProcessMessage(replyMessage);
			}
		}

		String replyToExchange = replyTo.getExchangeName();
		String replyToRoutingKey = replyTo.getRoutingKey();
		CompletableFuture<Boolean> sendFuture;
		if (StringUtils.hasText(replyToExchange)) {
			sendFuture = this.rabbitAmqpTemplate.send(replyToExchange, replyToRoutingKey, replyMessage);
		}
		else {
			Assert.hasText(replyToRoutingKey, "The 'replyTo' must be provided, in request message or in @SendTo.");
			sendFuture = this.rabbitAmqpTemplate.send(replyToRoutingKey.replaceFirst("queues/", ""), replyMessage);
		}

		sendFuture.join();
	}

	@Override
	protected void basicAck(Message request, @Nullable Channel channel) {
		AmqpAcknowledgment amqpAcknowledgment = request.getMessageProperties().getAmqpAcknowledgment();
		Assert.notNull(amqpAcknowledgment, "'(amqpAcknowledgment' must be provided into request message.");
		amqpAcknowledgment.acknowledge();
	}

	@Override
	public void onMessageBatch(List<Message> messages) {
		AmqpAcknowledgment amqpAcknowledgment =
				messages.stream()
						.findAny()
						.map((message) -> message.getMessageProperties().getAmqpAcknowledgment())
						.orElse(null);

		org.springframework.messaging.Message<?> converted;
		if (this.messagingMessageConverter.isAmqpMessageList()) {
			converted = new GenericMessage<>(messages);
		}
		else {
			List<? extends org.springframework.messaging.Message<?>> messagingMessages =
					messages.stream()
							.map(this::toMessagingMessage)
							.toList();

			if (this.messagingMessageConverter.isMessageList()) {
				converted = new GenericMessage<>(messagingMessages);
			}
			else {
				List<Object> payloads = new ArrayList<>();
				for (org.springframework.messaging.Message<?> message : messagingMessages) {
					payloads.add(message.getPayload());
				}
				converted = new GenericMessage<>(payloads);
			}
		}
		try {
			InvocationResult result = getHandlerAdapter()
					.invoke(converted, amqpAcknowledgment);
			if (result.getReturnValue() != null) {
				logger.warn("Replies for batches are not currently supported with RabbitMQ AMQP 1.0 listeners");
			}
		}
		catch (Exception ex) {
			throw new ListenerExecutionFailedException("Failed to invoke listener", ex,
					messages.toArray(new Message[0]));
		}
	}

}
