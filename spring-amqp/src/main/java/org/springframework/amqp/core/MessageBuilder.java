/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.amqp.core;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.springframework.util.Assert;

/**
 * Builds a Spring AMQP Message either from a byte[] body or
 * another Message using a fluent API.
 *
 * @author Gary Russell
 * @since 1.3
 *
 */
public final class MessageBuilder {

	private final byte[] body;

	private MessagePropertiesBuilder propertiesBuilder = MessagePropertiesBuilder.newInstance();

	/**
	 * The final message body will be a direct reference to 'body'.
	 * @param body The body.
	 * @return The builder.
	 */
	public static MessageBuilder withBody(byte[] body) {
		Assert.notNull(body, "'body' cannot be null");
		return new MessageBuilder(body);
	}

	/**
	 * The final message body will be a copy of 'body' in a new array.
	 * @param body The body.
	 * @return The builder.
	 */
	public static MessageBuilder withClonedBody(byte[] body) {
		Assert.notNull(body, "'body' cannot be null");
		return new MessageBuilder(Arrays.copyOf(body, body.length));
	}

	/**
	 * The final message body will be a new array containing the byte range from
	 * 'body'.
	 * @param body The body.
	 * @param from The starting index.
	 * @param to The ending index.
	 * @return The builder.
	 *
	 * @see Arrays#copyOfRange(byte[], int, int)
	 */
	public static MessageBuilder withBody(byte[] body, int from, int to) {
		Assert.notNull(body, "'body' cannot be null");
		return new MessageBuilder(Arrays.copyOfRange(body, from, to));
	}

	/**
	 * The final message body will be a direct reference to the message
	 * body, the MessageProperties will be a shallow copy.
	 * @param message The message.
	 * @return The builder.
	 */
	public static MessageBuilder fromMessage(Message message) {
		Assert.notNull(message, "'message' cannot be null");
		return new MessageBuilder(message);
	}

	/**
	 * The final message will have a copy of the message
	 * body, the MessageProperties will be cloned (top level only).
	 * @param message The message.
	 * @return The builder.
	 */
	public static MessageBuilder fromClonedMessage(Message message) {
		Assert.notNull(message, "'message' cannot be null");
		byte[] body = message.getBody();
		Assert.notNull(body, "'body' cannot be null");
		return new MessageBuilder(Arrays.copyOf(body, body.length), message.getMessageProperties());
	}

	private MessageBuilder(byte[] body) {
		this.body = body;
	}

	private MessageBuilder(Message message) {
		this(message.getBody(), message.getMessageProperties());
	}

	private MessageBuilder(byte[] body, MessageProperties properties) {
		this.body = body;
		this.copyProperties(properties);
	}

	/**
	 * Makes this builder's properties builder use a reference to properties.
	 * @param properties The properties.
	 * @return this.
	 */
	public MessageBuilder andProperties(MessageProperties properties) {
		this.propertiesBuilder = MessagePropertiesBuilder.fromProperties(properties);
		return this;
	}

	/*
	 * Delegate methods for property builder so users can use fluency with fromMessage...
	 *
	 * MessageBuilder.fromMessage(message).setHeaderIfAbsent("foo", "bar").build();
	 *
	 */

	public MessageBuilder setHeader(String key, Object value) {
		this.propertiesBuilder.setHeader(key, value);
		return this;
	}

	public MessageBuilder setTimestamp(Date timestamp) {
		this.propertiesBuilder.setTimestamp(timestamp);
		return this;
	}

	public MessageBuilder setMessageId(String messageId) {
		this.propertiesBuilder.setMessageId(messageId);
		return this;
	}

	public MessageBuilder setUserId(String userId) {
		this.propertiesBuilder.setUserId(userId);
		return this;
	}

	public MessageBuilder setAppId(String appId) {
		this.propertiesBuilder.setAppId(appId);
		return this;
	}

	public MessageBuilder setClusterId(String clusterId) {
		this.propertiesBuilder.setClusterId(clusterId);
		return this;
	}

	public MessageBuilder setType(String type) {
		this.propertiesBuilder.setType(type);
		return this;
	}

	public MessageBuilder setCorrelationId(byte[] correlationId) {
		this.propertiesBuilder.setCorrelationId(correlationId);
		return this;
	}

	public MessageBuilder setReplyTo(String replyTo) {
		this.propertiesBuilder.setReplyTo(replyTo);
		return this;
	}

	public MessageBuilder setReplyToAddress(Address replyTo) {
		this.propertiesBuilder.setReplyToAddress(replyTo);
		return this;
	}

	public MessageBuilder setContentType(String contentType) {
		this.propertiesBuilder.setContentType(contentType);
		return this;
	}

	public MessageBuilder setContentEncoding(String contentEncoding) {
		this.propertiesBuilder.setContentEncoding(contentEncoding);
		return this;
	}

	public MessageBuilder setContentLength(long contentLength) {
		this.propertiesBuilder.setContentLength(contentLength);
		return this;
	}

	public MessageBuilder setDeliveryMode(MessageDeliveryMode deliveryMode) {
		this.propertiesBuilder.setDeliveryMode(deliveryMode);
		return this;
	}

	public MessageBuilder setExpiration(String expiration) {
		this.propertiesBuilder.setExpiration(expiration);
		return this;
	}

	public MessageBuilder setPriority(Integer priority) {
		this.propertiesBuilder.setPriority(priority);
		return this;
	}

	public MessageBuilder setReceivedExchange(String receivedExchange) {
		this.propertiesBuilder.setReceivedExchange(receivedExchange);
		return this;
	}

	public MessageBuilder setReceivedRoutingKey(String receivedRoutingKey) {
		this.propertiesBuilder.setReceivedRoutingKey(receivedRoutingKey);
		return this;
	}

	public MessageBuilder setRedelivered(Boolean redelivered) {
		this.propertiesBuilder.setRedelivered(redelivered);
		return this;
	}

	public MessageBuilder setDeliveryTag(Long deliveryTag) {
		this.propertiesBuilder.setDeliveryTag(deliveryTag);
		return this;
	}

	public MessageBuilder setMessageCount(Integer messageCount) {
		this.propertiesBuilder.setMessageCount(messageCount);
		return this;
	}

	public MessageBuilder setHeaderIfAbsent(String key, Object value) {
		this.propertiesBuilder.setHeaderIfAbsent(key, value);
		return this;
	}

	public MessageBuilder setTimestampIfAbsent(Date timestamp) {
		this.propertiesBuilder.setTimestampIfAbsent(timestamp);
		return this;
	}

	public MessageBuilder setMessageIdIfAbsent(String messageId) {
		this.propertiesBuilder.setMessageIdIfAbsent(messageId);
		return this;
	}

	public MessageBuilder setUserIdIfAbsent(String userId) {
		this.propertiesBuilder.setUserIdIfAbsent(userId);
		return this;
	}

	public MessageBuilder setAppIdIfAbsent(String appId) {
		this.propertiesBuilder.setAppIdIfAbsent(appId);
		return this;
	}

	public MessageBuilder setClusterIdIfAbsent(String clusterId) {
		this.propertiesBuilder.setClusterIdIfAbsent(clusterId);
		return this;
	}

	public MessageBuilder setTypeIfAbsent(String type) {
		this.propertiesBuilder.setTypeIfAbsent(type);
		return this;
	}

	public MessageBuilder setCorrelationIdIfAbsent(byte[] correlationId) {
		this.propertiesBuilder.setCorrelationIdIfAbsent(correlationId);
		return this;
	}

	public MessageBuilder setReplyToIfAbsent(String replyTo) {
		this.propertiesBuilder.setReplyToIfAbsent(replyTo);
		return this;
	}

	public MessageBuilder setReplyToAddressIfAbsent(Address replyTo) {
		this.propertiesBuilder.setReplyToAddressIfAbsent(replyTo);
		return this;
	}

	public MessageBuilder setContentTypeIfAbsentOrDefault(String contentType) {
		this.propertiesBuilder.setContentTypeIfAbsentOrDefault(contentType);
		return this;
	}

	public MessageBuilder setContentEncodingIfAbsent(String contentEncoding) {
		this.propertiesBuilder.setContentEncodingIfAbsent(contentEncoding);
		return this;
	}

	public MessageBuilder setContentLengthIfAbsent(long contentLength) {
		this.propertiesBuilder.setContentLengthIfAbsent(contentLength);
		return this;
	}

	public MessageBuilder setDeliveryModeIfAbsentOrDefault(MessageDeliveryMode deliveryMode) {
		this.propertiesBuilder.setDeliveryModeIfAbsentOrDefault(deliveryMode);
		return this;
	}

	public MessageBuilder setExpirationIfAbsent(String expiration) {
		this.propertiesBuilder.setExpirationIfAbsent(expiration);
		return this;
	}

	public MessageBuilder setPriorityIfAbsentOrDefault(Integer priority) {
		this.propertiesBuilder.setPriorityIfAbsentOrDefault(priority);
		return this;
	}

	public MessageBuilder setReceivedExchangeIfAbsent(String receivedExchange) {
		this.propertiesBuilder.setReceivedExchangeIfAbsent(receivedExchange);
		return this;
	}

	public MessageBuilder setReceivedRoutingKeyIfAbsent(String receivedRoutingKey) {
		this.propertiesBuilder.setReceivedRoutingKeyIfAbsent(receivedRoutingKey);
		return this;
	}

	public MessageBuilder setRedeliveredIfAbsent(Boolean redelivered) {
		this.propertiesBuilder.setRedeliveredIfAbsent(redelivered);
		return this;
	}

	public MessageBuilder setDeliveryTagIfAbsent(Long deliveryTag) {
		this.propertiesBuilder.setDeliveryTagIfAbsent(deliveryTag);
		return this;
	}

	public MessageBuilder setMessageCountIfAbsent(Integer messageCount) {
		this.propertiesBuilder.setMessageCountIfAbsent(messageCount);
		return this;
	}

	public MessageBuilder copyProperties(MessageProperties properties) {
		this.propertiesBuilder.copyProperties(properties);
		return this;
	}

	public MessageBuilder copyHeaders(Map<String, Object> headers) {
		this.propertiesBuilder.copyHeaders(headers);
		return this;
	}

	public MessageBuilder copyHeadersIfAbsent(Map<String, Object> headers) {
		this.propertiesBuilder.copyHeadersIfAbsent(headers);
		return this;
	}

	public MessageBuilder removeHeader(String key) {
		this.propertiesBuilder.removeHeader(key);
		return this;
	}

	public MessageBuilder removeHeaders() {
		this.propertiesBuilder.removeHeaders();
		return this;
	}

	/*
	 * End delegate methods
	 */

	public Message build() {
		return new Message(this.body, this.propertiesBuilder.build());
	}

}
