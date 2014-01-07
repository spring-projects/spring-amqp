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

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.BeanUtils;

/**
 * Builds a Spring AMQP {@link MessageProperties} object using a
 * fluent API.
 *
 * @author Gary Russell
 * @since 1.3
 *
 */
public final class MessagePropertiesBuilder {

	private MessageProperties properties = new MessageProperties();

	/**
	 * Returns a builder with an initial set of properties.
	 * @return The builder.
	 */
	public static MessagePropertiesBuilder newInstance() {
		return new MessagePropertiesBuilder();
	}

	/**
	 * Initializes the builder with the supplied properties; the same
	 * object will be returned by {@link #build()}.
	 * @param properties The properties.
	 * @return The builder.
	 */
	public static MessagePropertiesBuilder fromProperties(MessageProperties properties) {
		return new MessagePropertiesBuilder(properties);
	}

	/**
	 * Performs a shallow copy of the properties for the initial value.
	 * @param properties The properties.
	 * @return The builder.
	 */
	public static MessagePropertiesBuilder fromClonedProperties(MessageProperties properties) {
		MessagePropertiesBuilder builder = newInstance();
		return builder.copyProperties(properties);
	}

	private MessagePropertiesBuilder() {
	}

	private MessagePropertiesBuilder(MessageProperties properties) {
		this.properties = properties;
	}

	public MessagePropertiesBuilder setHeader(String key, Object value) {
		this.properties.setHeader(key, value);
		return this;
	}

	public MessagePropertiesBuilder setTimestamp(Date timestamp) {
		this.properties.setTimestamp(timestamp);
		return this;
	}

	public MessagePropertiesBuilder setMessageId(String messageId) {
		this.properties.setMessageId(messageId);
		return this;
	}

	public MessagePropertiesBuilder setUserId(String userId) {
		this.properties.setUserId(userId);
		return this;
	}

	public MessagePropertiesBuilder setAppId(String appId) {
		this.properties.setAppId(appId);
		return this;
	}

	public MessagePropertiesBuilder setClusterId(String clusterId) {
		this.properties.setClusterId(clusterId);
		return this;
	}

	public MessagePropertiesBuilder setType(String type) {
		this.properties.setType(type);
		return this;
	}

	public MessagePropertiesBuilder setCorrelationId(byte[] correlationId) {
		this.properties.setCorrelationId(correlationId);
		return this;
	}

	public MessagePropertiesBuilder setReplyTo(String replyTo) {
		this.properties.setReplyTo(replyTo);
		return this;
	}

	public MessagePropertiesBuilder setReplyToAddress(Address replyTo) {
		this.properties.setReplyToAddress(replyTo);
		return this;
	}

	public MessagePropertiesBuilder setContentType(String contentType) {
		this.properties.setContentType(contentType);
		return this;
	}

	public MessagePropertiesBuilder setContentEncoding(String contentEncoding) {
		this.properties.setContentEncoding(contentEncoding);
		return this;
	}

	public MessagePropertiesBuilder setContentLength(long contentLength) {
		this.properties.setContentLength(contentLength);
		return this;
	}

	public MessagePropertiesBuilder setDeliveryMode(MessageDeliveryMode deliveryMode) {
		this.properties.setDeliveryMode(deliveryMode);
		return this;
	}

	public MessagePropertiesBuilder setExpiration(String expiration) {
		this.properties.setExpiration(expiration);
		return this;
	}

	public MessagePropertiesBuilder setPriority(Integer priority) {
		this.properties.setPriority(priority);
		return this;
	}

	public MessagePropertiesBuilder setReceivedExchange(String receivedExchange) {
		this.properties.setReceivedExchange(receivedExchange);
		return this;
	}

	public MessagePropertiesBuilder setReceivedRoutingKey(String receivedRoutingKey) {
		this.properties.setReceivedRoutingKey(receivedRoutingKey);
		return this;
	}

	public MessagePropertiesBuilder setRedelivered(Boolean redelivered) {
		this.properties.setRedelivered(redelivered);
		return this;
	}

	public MessagePropertiesBuilder setDeliveryTag(Long deliveryTag) {
		this.properties.setDeliveryTag(deliveryTag);
		return this;
	}

	public MessagePropertiesBuilder setMessageCount(Integer messageCount) {
		this.properties.setMessageCount(messageCount);
		return this;
	}

	/*
	 * *ifAbsent variants...
	 */

	public MessagePropertiesBuilder setHeaderIfAbsent(String key, Object value) {
		if (properties.getHeaders().get(key) == null) {
			this.properties.setHeader(key, value);
		}
		return this;
	}

	public MessagePropertiesBuilder setTimestampIfAbsent(Date timestamp) {
		if (this.properties.getTimestamp() == null) {
			this.properties.setTimestamp(timestamp);
		}
		return this;
	}

	public MessagePropertiesBuilder setMessageIdIfAbsent(String messageId) {
		if (this.properties.getMessageId() == null) {
			this.properties.setMessageId(messageId);
		}
		return this;
	}

	public MessagePropertiesBuilder setUserIdIfAbsent(String userId) {
		if (this.properties.getUserId() == null) {
			this.properties.setUserId(userId);
		}
		return this;
	}

	public MessagePropertiesBuilder setAppIdIfAbsent(String appId) {
		if (this.properties.getAppId() == null) {
			this.properties.setAppId(appId);
		}
		return this;
	}

	public MessagePropertiesBuilder setClusterIdIfAbsent(String clusterId) {
		if (this.properties.getClusterId() == null) {
			this.properties.setClusterId(clusterId);
		}
		return this;
	}

	public MessagePropertiesBuilder setTypeIfAbsent(String type) {
		if (this.properties.getType() == null) {
			this.properties.setType(type);
		}
		return this;
	}

	public MessagePropertiesBuilder setCorrelationIdIfAbsent(byte[] correlationId) {
		if (this.properties.getCorrelationId() == null) {
			this.properties.setCorrelationId(correlationId);
		}
		return this;
	}

	public MessagePropertiesBuilder setReplyToIfAbsent(String replyTo) {
		if (this.properties.getReplyTo() == null) {
			this.properties.setReplyTo(replyTo);
		}
		return this;
	}

	public MessagePropertiesBuilder setReplyToAddressIfAbsent(Address replyTo) {
		if (this.properties.getReplyToAddress() == null) {
			this.properties.setReplyToAddress(replyTo);
		}
		return this;
	}

	public MessagePropertiesBuilder setContentTypeIfAbsentOrDefault(String contentType) {
		if (this.properties.getContentType() == null
				|| this.properties.getContentType() == MessageProperties.DEFAULT_CONTENT_TYPE) {
			this.properties.setContentType(contentType);
		}
		return this;
	}

	public MessagePropertiesBuilder setContentEncodingIfAbsent(String contentEncoding) {
		if (this.properties.getContentEncoding() == null) {
			this.properties.setContentEncoding(contentEncoding);
		}
		return this;
	}

	public MessagePropertiesBuilder setContentLengthIfAbsent(long contentLength) {
		if (this.properties.getContentLength() == null) {
			this.properties.setContentLength(contentLength);
		}
		return this;
	}

	public MessagePropertiesBuilder setDeliveryModeIfAbsentOrDefault(MessageDeliveryMode deliveryMode) {
		if (this.properties.getDeliveryMode() == null
				|| this.properties.getDeliveryMode() == MessageProperties.DEFAULT_DELIVERY_MODE) {
			this.properties.setDeliveryMode(deliveryMode);
		}
		return this;
	}

	public MessagePropertiesBuilder setExpirationIfAbsent(String expiration) {
		if (this.properties.getExpiration() == null) {
			this.properties.setExpiration(expiration);
		}
		return this;
	}

	public MessagePropertiesBuilder setPriorityIfAbsentOrDefault(Integer priority) {
		if (this.properties.getPriority() == null
				|| this.properties.getPriority() == MessageProperties.DEFAULT_PRIORITY) {
			this.properties.setPriority(priority);
		}
		return this;
	}

	public MessagePropertiesBuilder setReceivedExchangeIfAbsent(String receivedExchange) {
		if (this.properties.getReceivedExchange() == null) {
			this.properties.setReceivedExchange(receivedExchange);
		}
		return this;
	}

	public MessagePropertiesBuilder setReceivedRoutingKeyIfAbsent(String receivedRoutingKey) {
		if (this.properties.getReceivedRoutingKey() == null) {
			this.properties.setReceivedRoutingKey(receivedRoutingKey);
		}
		return this;
	}

	public MessagePropertiesBuilder setRedeliveredIfAbsent(Boolean redelivered) {
		if (this.properties.isRedelivered() == null) {
			this.properties.setRedelivered(redelivered);
		}
		return this;
	}

	public MessagePropertiesBuilder setDeliveryTagIfAbsent(Long deliveryTag) {
		if (this.properties.getDeliveryTag() == null) {
			this.properties.setDeliveryTag(deliveryTag);
		}
		return this;
	}

	public MessagePropertiesBuilder setMessageCountIfAbsent(Integer messageCount) {
		if (this.properties.getMessageCount() == null) {
			this.properties.setMessageCount(messageCount);
		}
		return this;
	}

	public MessagePropertiesBuilder copyProperties(MessageProperties properties) {
		BeanUtils.copyProperties(properties, this.properties);
		// Special handling of replyTo needed because the format depends on how it was set
		this.properties.setReplyTo(properties.getReplyTo());
		this.properties.getHeaders().putAll(properties.getHeaders());
		return this;
	}

	public MessagePropertiesBuilder copyHeaders(Map<String, Object> headers) {
		this.properties.getHeaders().putAll(headers);
		return this;
	}

	public MessagePropertiesBuilder copyHeadersIfAbsent(Map<String, Object> headers) {
		Map<String, Object> existingHeaders = this.properties.getHeaders();
		for (Entry<String, Object> entry : headers.entrySet()) {
			if (!existingHeaders.containsKey(entry.getKey())) {
				existingHeaders.put(entry.getKey(), entry.getValue());
			}
		}
		return this;
	}

	public MessagePropertiesBuilder removeHeader(String key) {
		this.properties.getHeaders().remove(key);
		return this;
	}

	public MessagePropertiesBuilder removeHeaders() {
		this.properties.getHeaders().clear();
		return this;
	}

	public MessageProperties build() {
		return this.properties;
	}
}
