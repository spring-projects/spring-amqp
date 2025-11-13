/*
 * Copyright 2019-present the original author or authors.
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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.rabbitmq.client.Channel;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareBatchMessageListener;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbit.listener.support.ContainerUtils;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * A listener adapter for batch listeners.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.2
 *
 */
public class BatchMessagingMessageListenerAdapter extends MessagingMessageListenerAdapter
		implements ChannelAwareBatchMessageListener {

	private final BatchingStrategy batchingStrategy;

	@SuppressWarnings("this-escape")
	public BatchMessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method, boolean returnExceptions,
			@Nullable RabbitListenerErrorHandler errorHandler, @Nullable BatchingStrategy batchingStrategy) {

		super(bean, method, returnExceptions, errorHandler, true);
		this.batchingStrategy = batchingStrategy == null ? new SimpleBatchingStrategy(0, 0, 0L) : batchingStrategy;
	}

	@Override
	public void onMessageBatch(List<org.springframework.amqp.core.Message> messages, @Nullable Channel channel) {
		Message<?> converted;
		if (this.messagingMessageConverter.isAmqpMessageList()) {
			converted = new GenericMessage<>(messages);
		}
		else {
			List<Message<?>> messagingMessages = new ArrayList<>(messages.size());
			for (org.springframework.amqp.core.Message message : messages) {
				try {
					Message<?> messagingMessage = toMessagingMessage(message);
					messagingMessages.add(messagingMessage);
				}
				catch (MessageConversionException e) {
					this.logger.error("Could not convert incoming message", e);
					try {
						Assert.notNull(channel, "'channel' cannot be null");
						channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
					}
					catch (Exception ex) {
						this.logger.error("Failed to reject message with conversion error", ex);
						throw e; // NOSONAR
					}
				}
			}
			if (this.messagingMessageConverter.isMessageList()) {
				converted = new GenericMessage<>(messagingMessages);
			}
			else {
				List<Object> payloads = new ArrayList<>(messagingMessages.size());
				for (Message<?> message : messagingMessages) {
					payloads.add(message.getPayload());
				}
				converted = new GenericMessage<>(payloads);
			}
		}
		try {
			invokeHandlerAndProcessResult(messages, channel, converted);
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	private void invokeHandlerAndProcessResult(List<org.springframework.amqp.core.Message> amqpMessages,
			@Nullable Channel channel, Message<?> message) {

		if (logger.isDebugEnabled()) {
			logger.debug("Processing [" + message + "]");
		}
		InvocationResult result = invokeHandler(channel, message, true,
				amqpMessages.toArray(new org.springframework.amqp.core.Message[0]));
		if (result.getReturnValue() != null) {
			handleResult(result, amqpMessages, channel);
		}
		else {
			logger.trace("No result object given - no result to handle");
		}
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	private void handleResult(InvocationResult resultArg, List<org.springframework.amqp.core.Message> amqpMessages,
			@Nullable Channel channel) {

		if (channel != null) {
			if (resultArg.getReturnValue() instanceof CompletableFuture<?> completable) {
				if (!isManualAck()) {
					this.logger.warn("Container AcknowledgeMode must be MANUAL for a Future<?> return type; "
							+ "otherwise the container will ack the message immediately");
				}
				completable.whenComplete((r, t) -> {
					if (t == null) {
						amqpMessages.forEach((request) -> basicAck(request, channel));
					}
					else {
						asyncFailure(amqpMessages, channel, t);
					}
				});
			}
			else if (monoPresent && MonoHandler.isMono(resultArg.getReturnValue())) {
				if (!isManualAck()) {
					this.logger.warn("Container AcknowledgeMode must be MANUAL for a Mono<?> return type" +
							"(or Kotlin suspend function); otherwise the container will ack the message immediately");
				}
				MonoHandler.subscribe(resultArg.getReturnValue(),
						null,
						t -> asyncFailure(amqpMessages, channel, t),
						() -> amqpMessages.forEach((request) -> basicAck(request, channel)));
			}
			else {
				throw new IllegalStateException("The listener in batch mode does not support replies.");
			}
		}
		else if (this.logger.isWarnEnabled()) {
			this.logger.warn("Listener method returned result [" + resultArg
					+ "]: not generating response message for it because no Rabbit Channel given");
		}
	}

	private void asyncFailure(List<org.springframework.amqp.core.Message> requests, Channel channel, Throwable t) {
		this.logger.error("Future, Mono, or suspend function was completed with an exception for " + requests, t);
		for (org.springframework.amqp.core.Message request : requests) {
			try {
				channel.basicNack(request.getMessageProperties().getDeliveryTag(), false,
						ContainerUtils.shouldRequeue(isDefaultRequeueRejected(), t, this.logger));
			}
			catch (IOException e) {
				this.logger.error("Failed to nack message", e);
			}
		}
	}

	@Override
	protected Message<?> toMessagingMessage(org.springframework.amqp.core.Message amqpMessage) {
		if (this.batchingStrategy.canDebatch(amqpMessage.getMessageProperties())) {

			if (this.messagingMessageConverter.isMessageList()) {
				List<Message<?>> messages = new ArrayList<>();
				this.batchingStrategy.deBatch(amqpMessage, fragment -> messages.add(super.toMessagingMessage(fragment)));
				return new GenericMessage<>(messages);
			}
			else {
				List<Object> list = new ArrayList<>();
				this.batchingStrategy.deBatch(amqpMessage, fragment ->
						list.add(this.messagingMessageConverter.extractPayload(fragment)));
				return MessageBuilder.withPayload(list)
						.copyHeaders(this.messagingMessageConverter
								.getHeaderMapper()
								.toHeaders(amqpMessage.getMessageProperties()))
						.build();
			}
		}
		return super.toMessagingMessage(amqpMessage);
	}

}
