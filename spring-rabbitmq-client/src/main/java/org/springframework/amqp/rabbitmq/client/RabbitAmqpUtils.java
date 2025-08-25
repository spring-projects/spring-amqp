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

package org.springframework.amqp.rabbitmq.client;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import com.rabbitmq.client.amqp.Consumer;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.util.StringUtils;

/**
 * The utilities for RabbitMQ AMQP 1.0 protocol API.
 */
public final class RabbitAmqpUtils {

	/**
	 * Convert {@link com.rabbitmq.client.amqp.Message} into {@link Message}.
	 * @param amqpMessage the {@link com.rabbitmq.client.amqp.Message} convert from.
	 * @param context the {@link Consumer.Context} for manual message settlement.
	 * @return the {@link Message} mapped from a {@link com.rabbitmq.client.amqp.Message}.
	 */
	public static Message fromAmqpMessage(com.rabbitmq.client.amqp.Message amqpMessage,
			Consumer.@Nullable Context context) {

		MessageProperties messageProperties = new MessageProperties();

		JavaUtils.INSTANCE
				.acceptIfNotNull(amqpMessage.messageIdAsString(), messageProperties::setMessageId)
				.acceptIfNotNull(amqpMessage.userId(),
						(usr) -> messageProperties.setUserId(new String(usr, StandardCharsets.UTF_8)))
				.acceptIfNotNull(amqpMessage.correlationIdAsString(), messageProperties::setCorrelationId)
				.acceptIfNotNull(amqpMessage.contentType(), messageProperties::setContentType)
				.acceptIfNotNull(amqpMessage.contentEncoding(), messageProperties::setContentEncoding)
				.acceptIfNotNull(amqpMessage.replyTo(), messageProperties::setReplyTo);

		long creationTime = amqpMessage.creationTime();
		if (creationTime <= 0) {
			creationTime = System.currentTimeMillis();
		}
		messageProperties.setTimestamp(new Date(creationTime));

		long absoluteExpiryTime = amqpMessage.absoluteExpiryTime();
		if (absoluteExpiryTime > creationTime) {
			messageProperties.setExpiration(Long.toString(absoluteExpiryTime - creationTime));
		}

		amqpMessage.forEachProperty(messageProperties::setHeader);

		if (context != null) {
			messageProperties.setAmqpAcknowledgment((status) -> {
				switch (status) {
					case ACCEPT -> context.accept();
					case REJECT -> context.discard();
					case REQUEUE -> context.requeue();
				}
			});
		}

		return new Message(amqpMessage.body(), messageProperties);
	}

	/**
	 * Convert {@link Message} into {@link com.rabbitmq.client.amqp.Message}.
	 * The {@link MessageProperties#getReplyTo()} is set into {@link com.rabbitmq.client.amqp.Message#to(String)}.
	 * The {@link com.rabbitmq.client.amqp.Message#correlationId(long)} is set to
	 * {@link MessageProperties#getCorrelationId()} if present, or to {@link MessageProperties#getMessageId()}.
	 * @param message the {@link Message} convert from.
	 * @param amqpMessage the {@link com.rabbitmq.client.amqp.Message} convert into.
	 */
	public static void toAmqpMessage(Message message, com.rabbitmq.client.amqp.Message amqpMessage) {
		MessageProperties messageProperties = message.getMessageProperties();

		amqpMessage
				.body(message.getBody())
				.contentEncoding(messageProperties.getContentEncoding())
				.contentType(messageProperties.getContentType())
				.messageId(messageProperties.getMessageId())
				.priority(messageProperties.getPriority().byteValue());

		Map<String, @Nullable Object> headers = messageProperties.getHeaders();
		headers.forEach((key, val) -> mapProp(key, val, amqpMessage));

		JavaUtils.INSTANCE
				.acceptOrElseIfNotNull(messageProperties.getCorrelationId(),
						messageProperties.getMessageId(), amqpMessage::correlationId)
				.acceptOrElseIfNotNull(messageProperties.getTimestamp(),
						new Date(), (timestamp) -> amqpMessage.creationTime(timestamp.getTime()))
				.acceptIfNotNull(messageProperties.getUserId(),
						(userId) -> amqpMessage.userId(userId.getBytes(StandardCharsets.UTF_8)))
				.acceptIfNotNull(messageProperties.getReplyTo(), amqpMessage::to);

		String expiration = messageProperties.getExpiration();
		if (StringUtils.hasText(expiration)) {
			amqpMessage.absoluteExpiryTime(amqpMessage.creationTime() + Long.parseLong(expiration));
		}
	}

	private static void mapProp(String key, @Nullable Object val, com.rabbitmq.client.amqp.Message amqpMessage) {
		if (val == null) {
			return;
		}
		if (val instanceof String string) {
			amqpMessage.property(key, string);
		}
		else if (val instanceof Long longValue) {
			amqpMessage.property(key, longValue);
		}
		else if (val instanceof Integer intValue) {
			amqpMessage.property(key, intValue);
		}
		else if (val instanceof Short shortValue) {
			amqpMessage.property(key, shortValue);
		}
		else if (val instanceof Byte byteValue) {
			amqpMessage.property(key, byteValue);
		}
		else if (val instanceof Double doubleValue) {
			amqpMessage.property(key, doubleValue);
		}
		else if (val instanceof Float floatValue) {
			amqpMessage.property(key, floatValue);
		}
		else if (val instanceof Character character) {
			amqpMessage.property(key, character);
		}
		else if (val instanceof UUID uuid) {
			amqpMessage.property(key, uuid);
		}
		else if (val instanceof byte[] bytes) {
			amqpMessage.property(key, bytes);
		}
		else if (val instanceof Boolean booleanValue) {
			amqpMessage.property(key, booleanValue);
		}
	}

	private RabbitAmqpUtils() {
	}

}
