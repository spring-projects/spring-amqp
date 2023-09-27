/*
 * Copyright 2019-2023 the original author or authors.
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
import java.util.ArrayList;
import java.util.List;

import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareBatchMessageListener;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;

import com.rabbitmq.client.Channel;

/**
 * A listener adapter for batch listeners.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public class BatchMessagingMessageListenerAdapter extends MessagingMessageListenerAdapter
			implements ChannelAwareBatchMessageListener {

	private final MessagingMessageConverterAdapter converterAdapter;

	private final BatchingStrategy batchingStrategy;

	public BatchMessagingMessageListenerAdapter(Object bean, Method method, boolean returnExceptions,
			RabbitListenerErrorHandler errorHandler, @Nullable BatchingStrategy batchingStrategy) {

		super(bean, method, returnExceptions, errorHandler, true);
		this.converterAdapter = (MessagingMessageConverterAdapter) getMessagingMessageConverter();
		this.batchingStrategy = batchingStrategy == null ? new SimpleBatchingStrategy(0, 0, 0L) : batchingStrategy;
	}

	@Override
	public void onMessageBatch(List<org.springframework.amqp.core.Message> messages, Channel channel) {
		Message<?> converted;
		if (this.converterAdapter.isAmqpMessageList()) {
			converted = new GenericMessage<>(messages);
		}
		else {
			List<Message<?>> messagingMessages = new ArrayList<>();
			for (org.springframework.amqp.core.Message message : messages) {
				try {
					Message<?> messagingMessage = toMessagingMessage(message);
					messagingMessages.add(messagingMessage);
				}
				catch (MessageConversionException e) {
					this.logger.error("Could not convert incoming message", e);
					try {
						channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
					}
					catch (Exception ex) {
						this.logger.error("Failed to reject message with conversion error", ex);
						throw e; // NOSONAR
					}
				}
			}
			if (this.converterAdapter.isMessageList()) {
				converted = new GenericMessage<>(messagingMessages);
			}
			else {
				List<Object> payloads = new ArrayList<>();
				for (Message<?> message : messagingMessages) {
					payloads.add(message.getPayload());
				}
				converted = new GenericMessage<>(payloads);
			}
		}
		try {
			invokeHandlerAndProcessResult(null, channel, converted);
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	@Override
	protected Message<?> toMessagingMessage(org.springframework.amqp.core.Message amqpMessage) {
		if (this.batchingStrategy.canDebatch(amqpMessage.getMessageProperties())) {

			if (this.converterAdapter.isMessageList()) {
				List<Message<?>> messages = new ArrayList<>();
				this.batchingStrategy.deBatch(amqpMessage, fragment -> {
					messages.add(super.toMessagingMessage(fragment));
				});
				return new GenericMessage<>(messages);
			}
			else {
				List<Object> list = new ArrayList<>();
				this.batchingStrategy.deBatch(amqpMessage, fragment -> {
					list.add(this.converterAdapter.extractPayload(fragment));
				});
				return MessageBuilder.withPayload(list)
						.copyHeaders(this.converterAdapter
								.getHeaderMapper()
								.toHeaders(amqpMessage.getMessageProperties()))
						.build();
			}
		}
		return super.toMessagingMessage(amqpMessage);
	}

}
