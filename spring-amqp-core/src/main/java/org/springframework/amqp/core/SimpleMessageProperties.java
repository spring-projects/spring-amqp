/*
 * Copyright 2002-2010 the original author or authors.
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
import java.util.HashMap;
import java.util.Map;

/**
 * @author Mark Fisher
 */
public class SimpleMessageProperties implements MessageProperties {

	private static final String DEFAULT_CHARSET = "UTF-8";


	private volatile String defaultCharset = DEFAULT_CHARSET;

	private volatile String appId;

	private volatile String clusterId;

	private volatile String contentEncoding;

	private volatile long contentLength;

	private volatile String contentType = MessageProperties.CONTENT_TYPE_BYTES;

	private volatile byte[] correlationId;

	private volatile MessageDeliveryMode deliveryMode;

	private volatile long deliveryTag;

	private volatile String expiration;

	private volatile Map<String, Object> headers = new HashMap<String, Object>();

	private volatile Integer messageCount;

	private volatile String messageId;

	private volatile Integer priority;

	private volatile String receivedExchange;

	private volatile String receivedRoutingKey;

	private volatile Address replyTo;

	private volatile Date timestamp;

	private volatile String type;

	private volatile String userId;

	private volatile Boolean redelivered;


	public String getAppId() {
		return this.appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getClusterId() {
		return this.clusterId;
	}

	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	public String getContentEncoding() {
		return this.contentEncoding;
	}

	public void setContentEncoding(String contentEncoding) {
		this.contentEncoding = contentEncoding;
	}

	public long getContentLength() {
		return this.contentLength;
	}

	public void setContentLength(long contentLength) {
		this.contentLength = contentLength;
	}

	public String getContentType() {
		return this.contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public byte[] getCorrelationId() {
		return this.correlationId;
	}

	public void setCorrelationId(byte[] correlationId) {
		this.correlationId = correlationId;
	}

	public MessageDeliveryMode getDeliveryMode() {
		return this.deliveryMode;
	}

	public void setDeliveryMode(MessageDeliveryMode deliveryMode) {
		this.deliveryMode = deliveryMode;
	}

	public long getDeliveryTag() {
		return this.deliveryTag;
	}

	public void setDeliveryTag(long deliveryTag) {
		this.deliveryTag = deliveryTag;
	}

	public String getExpiration() {
		return this.expiration;
	}

	public void setExpiration(String expiration) {
		this.expiration = expiration;
	}

	public Map<String, Object> getHeaders() {
		return this.headers;
	}

	public void setHeaders(Map<String, Object> headers) {
		this.headers = headers;
	}

	public Integer getMessageCount() {
		return this.messageCount;
	}

	public void setMessageCount(Integer messageCount) {
		this.messageCount = messageCount;
	}

	public String getMessageId() {
		return this.messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public Integer getPriority() {
		return this.priority;
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	public String getReceivedExchange() {
		return this.receivedExchange;
	}

	public void setReceivedExchange(String receivedExchange) {
		this.receivedExchange = receivedExchange;
	}

	public String getReceivedRoutingKey() {
		return this.receivedRoutingKey;
	}

	public void setReceivedRoutingKey(String receivedRoutingKey) {
		this.receivedRoutingKey = receivedRoutingKey;
	}

	public Address getReplyTo() {
		return this.replyTo;
	}

	public void setReplyTo(Address replyTo) {
		this.replyTo = replyTo;
	}

	public Date getTimestamp() {
		return this.timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getUserId() {
		return this.userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Boolean isRedelivered() {
		return this.redelivered;
	}

	public void setRedelivered(Boolean redelivered) {
		this.redelivered = redelivered;
	}

	public void setDefaultCharset(String defaultCharset) {
		this.defaultCharset = defaultCharset;
	}

	public void setHeader(String key, Object value) {
		this.headers.put(key, value);
	}

}
