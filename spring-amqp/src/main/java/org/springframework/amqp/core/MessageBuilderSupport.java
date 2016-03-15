/*
 * Copyright 2014-2016 the original author or authors.
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
 * Support class for building {@link Message} and {@link MessageProperties}
 * fluent API.
 *
 * @author Gary Russell
 * @since 1.3
 *
 */
public abstract class MessageBuilderSupport<T> {

	private MessageProperties properties = new MessageProperties();

	protected MessageBuilderSupport() {
	}

	protected MessageBuilderSupport(MessageProperties properties) {
		this.properties = properties;
	}

	protected void setProperties(MessageProperties properties) {
		this.properties = properties;
	}

	public MessageBuilderSupport<T> setHeader(String key, Object value) {
		this.properties.setHeader(key, value);
		return this;
	}

	public MessageBuilderSupport<T> setTimestamp(Date timestamp) {
		this.properties.setTimestamp(timestamp);
		return this;
	}

	public MessageBuilderSupport<T> setMessageId(String messageId) {
		this.properties.setMessageId(messageId);
		return this;
	}

	public MessageBuilderSupport<T> setUserId(String userId) {
		this.properties.setUserId(userId);
		return this;
	}

	public MessageBuilderSupport<T> setAppId(String appId) {
		this.properties.setAppId(appId);
		return this;
	}

	public MessageBuilderSupport<T> setClusterId(String clusterId) {
		this.properties.setClusterId(clusterId);
		return this;
	}

	public MessageBuilderSupport<T> setType(String type) {
		this.properties.setType(type);
		return this;
	}

	public MessageBuilderSupport<T> setCorrelationId(byte[] correlationId) {
		this.properties.setCorrelationId(correlationId);
		return this;
	}

	public MessageBuilderSupport<T> setReplyTo(String replyTo) {
		this.properties.setReplyTo(replyTo);
		return this;
	}

	public MessageBuilderSupport<T> setReplyToAddress(Address replyTo) {
		this.properties.setReplyToAddress(replyTo);
		return this;
	}

	public MessageBuilderSupport<T> setContentType(String contentType) {
		this.properties.setContentType(contentType);
		return this;
	}

	public MessageBuilderSupport<T> setContentEncoding(String contentEncoding) {
		this.properties.setContentEncoding(contentEncoding);
		return this;
	}

	public MessageBuilderSupport<T> setContentLength(long contentLength) {
		this.properties.setContentLength(contentLength);
		return this;
	}

	public MessageBuilderSupport<T> setDeliveryMode(MessageDeliveryMode deliveryMode) {
		this.properties.setDeliveryMode(deliveryMode);
		return this;
	}

	public MessageBuilderSupport<T> setExpiration(String expiration) {
		this.properties.setExpiration(expiration);
		return this;
	}

	public MessageBuilderSupport<T> setPriority(Integer priority) {
		this.properties.setPriority(priority);
		return this;
	}

	public MessageBuilderSupport<T> setReceivedExchange(String receivedExchange) {
		this.properties.setReceivedExchange(receivedExchange);
		return this;
	}

	public MessageBuilderSupport<T> setReceivedRoutingKey(String receivedRoutingKey) {
		this.properties.setReceivedRoutingKey(receivedRoutingKey);
		return this;
	}

	public MessageBuilderSupport<T> setRedelivered(Boolean redelivered) {
		this.properties.setRedelivered(redelivered);
		return this;
	}

	public MessageBuilderSupport<T> setDeliveryTag(Long deliveryTag) {
		this.properties.setDeliveryTag(deliveryTag);
		return this;
	}

	public MessageBuilderSupport<T> setMessageCount(Integer messageCount) {
		this.properties.setMessageCount(messageCount);
		return this;
	}

	/*
	 * *ifAbsent variants...
	 */

	public MessageBuilderSupport<T> setHeaderIfAbsent(String key, Object value) {
		if (this.properties.getHeaders().get(key) == null) {
			this.properties.setHeader(key, value);
		}
		return this;
	}

	public MessageBuilderSupport<T> setTimestampIfAbsent(Date timestamp) {
		if (this.properties.getTimestamp() == null) {
			this.properties.setTimestamp(timestamp);
		}
		return this;
	}

	public MessageBuilderSupport<T> setMessageIdIfAbsent(String messageId) {
		if (this.properties.getMessageId() == null) {
			this.properties.setMessageId(messageId);
		}
		return this;
	}

	public MessageBuilderSupport<T> setUserIdIfAbsent(String userId) {
		if (this.properties.getUserId() == null) {
			this.properties.setUserId(userId);
		}
		return this;
	}

	public MessageBuilderSupport<T> setAppIdIfAbsent(String appId) {
		if (this.properties.getAppId() == null) {
			this.properties.setAppId(appId);
		}
		return this;
	}

	public MessageBuilderSupport<T> setClusterIdIfAbsent(String clusterId) {
		if (this.properties.getClusterId() == null) {
			this.properties.setClusterId(clusterId);
		}
		return this;
	}

	public MessageBuilderSupport<T> setTypeIfAbsent(String type) {
		if (this.properties.getType() == null) {
			this.properties.setType(type);
		}
		return this;
	}

	public MessageBuilderSupport<T> setCorrelationIdIfAbsent(byte[] correlationId) {
		if (this.properties.getCorrelationId() == null) {
			this.properties.setCorrelationId(correlationId);
		}
		return this;
	}

	public MessageBuilderSupport<T> setReplyToIfAbsent(String replyTo) {
		if (this.properties.getReplyTo() == null) {
			this.properties.setReplyTo(replyTo);
		}
		return this;
	}

	public MessageBuilderSupport<T> setReplyToAddressIfAbsent(Address replyTo) {
		if (this.properties.getReplyToAddress() == null) {
			this.properties.setReplyToAddress(replyTo);
		}
		return this;
	}

	public MessageBuilderSupport<T> setContentTypeIfAbsentOrDefault(String contentType) {
		if (this.properties.getContentType() == null
				|| this.properties.getContentType().equals(MessageProperties.DEFAULT_CONTENT_TYPE)) {
			this.properties.setContentType(contentType);
		}
		return this;
	}

	public MessageBuilderSupport<T> setContentEncodingIfAbsent(String contentEncoding) {
		if (this.properties.getContentEncoding() == null) {
			this.properties.setContentEncoding(contentEncoding);
		}
		return this;
	}

	public MessageBuilderSupport<T> setContentLengthIfAbsent(long contentLength) {
		if (!this.properties.isContentLengthSet()) {
			this.properties.setContentLength(contentLength);
		}
		return this;
	}

	public MessageBuilderSupport<T> setDeliveryModeIfAbsentOrDefault(MessageDeliveryMode deliveryMode) {
		if (this.properties.getDeliveryMode() == null
				|| this.properties.getDeliveryMode().equals(MessageProperties.DEFAULT_DELIVERY_MODE)) {
			this.properties.setDeliveryMode(deliveryMode);
		}
		return this;
	}

	public MessageBuilderSupport<T> setExpirationIfAbsent(String expiration) {
		if (this.properties.getExpiration() == null) {
			this.properties.setExpiration(expiration);
		}
		return this;
	}

	public MessageBuilderSupport<T> setPriorityIfAbsentOrDefault(Integer priority) {
		if (this.properties.getPriority() == null
				|| MessageProperties.DEFAULT_PRIORITY.equals(this.properties.getPriority())) {
			this.properties.setPriority(priority);
		}
		return this;
	}

	public MessageBuilderSupport<T> setReceivedExchangeIfAbsent(String receivedExchange) {
		if (this.properties.getReceivedExchange() == null) {
			this.properties.setReceivedExchange(receivedExchange);
		}
		return this;
	}

	public MessageBuilderSupport<T> setReceivedRoutingKeyIfAbsent(String receivedRoutingKey) {
		if (this.properties.getReceivedRoutingKey() == null) {
			this.properties.setReceivedRoutingKey(receivedRoutingKey);
		}
		return this;
	}

	public MessageBuilderSupport<T> setRedeliveredIfAbsent(Boolean redelivered) {
		if (this.properties.isRedelivered() == null) {
			this.properties.setRedelivered(redelivered);
		}
		return this;
	}

	public MessageBuilderSupport<T> setDeliveryTagIfAbsent(Long deliveryTag) {
		if (!this.properties.isDeliveryTagSet()) {
			this.properties.setDeliveryTag(deliveryTag);
		}
		return this;
	}

	public MessageBuilderSupport<T> setMessageCountIfAbsent(Integer messageCount) {
		if (this.properties.getMessageCount() == null) {
			this.properties.setMessageCount(messageCount);
		}
		return this;
	}

	public MessageBuilderSupport<T> copyProperties(MessageProperties properties) {
		BeanUtils.copyProperties(properties, this.properties);
		// Special handling of replyTo needed because the format depends on how it was set
		this.properties.setReplyTo(properties.getReplyTo());
		this.properties.getHeaders().putAll(properties.getHeaders());
		return this;
	}

	public MessageBuilderSupport<T> copyHeaders(Map<String, Object> headers) {
		this.properties.getHeaders().putAll(headers);
		return this;
	}

	public MessageBuilderSupport<T> copyHeadersIfAbsent(Map<String, Object> headers) {
		Map<String, Object> existingHeaders = this.properties.getHeaders();
		for (Entry<String, Object> entry : headers.entrySet()) {
			if (!existingHeaders.containsKey(entry.getKey())) {
				existingHeaders.put(entry.getKey(), entry.getValue());
			}
		}
		return this;
	}

	public MessageBuilderSupport<T> removeHeader(String key) {
		this.properties.getHeaders().remove(key);
		return this;
	}

	public MessageBuilderSupport<T> removeHeaders() {
		this.properties.getHeaders().clear();
		return this;
	}

	protected MessageProperties buildProperties() {
		return this.properties;
	}

	public abstract T build();
}
