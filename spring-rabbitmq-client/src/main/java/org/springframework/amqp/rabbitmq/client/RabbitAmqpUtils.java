/*
 * Copyright 2025-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import com.rabbitmq.client.amqp.Consumer;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.util.StringUtils;

/**
 * The utilities for RabbitMQ AMQP 1.0 protocol API.
 */
public final class RabbitAmqpUtils {

	private static final long NO_TTL = 0xFFFF_FFFFL;

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
				.acceptIfNotNull(amqpMessage.messageId(),
						(messageId) -> messageProperties.setMessageId(messageId.toString()))
				.acceptIfNotNull(amqpMessage.userId(),
						(usr) -> messageProperties.setUserId(new String(usr, StandardCharsets.UTF_8)))
				.acceptIfNotNull(amqpMessage.correlationId(),
						(correlationId) -> messageProperties.setCorrelationId(correlationId.toString()))
				.acceptIfNotNull(amqpMessage.contentType(), messageProperties::setContentType)
				.acceptIfNotNull(amqpMessage.contentEncoding(), messageProperties::setContentEncoding)
				.acceptIfNotNull(amqpMessage.replyTo(), messageProperties::setReplyTo)
				.acceptIfNotNull(amqpMessage.annotation("x-exchange"),
						(exchange) -> messageProperties.setReceivedExchange(exchange.toString()))
				.acceptIfNotNull(amqpMessage.annotation("x-routing-key"),
						(routingKey) -> messageProperties.setReceivedRoutingKey(routingKey.toString()));

		messageProperties.setPriority(Byte.valueOf(amqpMessage.priority()).intValue());
		messageProperties.setDeliveryMode(amqpMessage.durable()
				? MessageDeliveryMode.PERSISTENT
				: MessageDeliveryMode.NON_PERSISTENT);

		long creationTime = amqpMessage.creationTime();
		if (creationTime <= 0) {
			creationTime = System.currentTimeMillis();
		}
		messageProperties.setTimestamp(new Date(creationTime));

		long ttl = amqpMessage.ttl().toMillis();
		if (ttl != NO_TTL) {
			messageProperties.setExpiration(Long.toString(ttl));
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
	 * The {@link MessageProperties#getReplyTo()} is set into {@link com.rabbitmq.client.amqp.Message#replyTo(String)}.
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
				.priority(messageProperties.getPriority().byteValue())
				.durable(MessageDeliveryMode.PERSISTENT.equals(messageProperties.getDeliveryMode()));

		Map<String, @Nullable Object> headers = messageProperties.getHeaders();
		headers.forEach((key, val) -> mapProp(key, val, amqpMessage));

		JavaUtils.INSTANCE
				.acceptOrElseIfNotNull(messageProperties.getCorrelationId(),
						messageProperties.getMessageId(), amqpMessage::correlationId)
				.acceptOrElseIfNotNull(messageProperties.getTimestamp(),
						new Date(), (timestamp) -> amqpMessage.creationTime(timestamp.getTime()))
				.acceptIfNotNull(messageProperties.getUserId(),
						(userId) -> amqpMessage.userId(userId.getBytes(StandardCharsets.UTF_8)))
				.acceptIfNotNull(messageProperties.getReplyTo(), amqpMessage::replyTo);

		String expiration = messageProperties.getExpiration();
		if (StringUtils.hasText(expiration)) {
			amqpMessage.ttl(Duration.ofMillis(Long.parseLong(expiration)));
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
		else if (val instanceof Date date) {
			amqpMessage.propertyTimestamp(key, date.getTime());
		}
	}

	private RabbitAmqpUtils() {
	}

}
