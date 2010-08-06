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

package org.springframework.amqp.rabbit.core;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.AmqpUnsupportedEncodingException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.Assert;

import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * Rabbit implementation of MessageProperties that stores much of the message property
 * information in Rabbit's {@link BasicProperties} class. Empty headers will be created
 * on demand if they are null in the underlying BasicProperties instance.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
public class RabbitMessageProperties implements MessageProperties {

	private static final String DEFAULT_CHARSET = "UTF-8";

	private volatile String defaultCharset = DEFAULT_CHARSET;

	private volatile String receivedExchange;

	private volatile String routingKey;

	private volatile Boolean redelivered;

	private volatile long deliveryTag;

	private volatile int messageCount;

	private volatile long contentLength;
	
	private final BasicProperties basicProperties;


	public RabbitMessageProperties() {
		//TODO do we want this to be the default?
		this.basicProperties = new BasicProperties("application/octet-stream",
                null,
                null,
                2,
                0, null, null, null,
                null, null, null, null,
                null, null);
		initializeHeadersIfNecessary(basicProperties);
	}

	/**
	 * @param basicProperties
	 * @param receivedExchange
	 * @param receivedRoutingKey
	 * @param redelivered
	 * @param deliveryTag
	 * @param messageCount
	 */
	public RabbitMessageProperties(BasicProperties basicProperties, String receivedExchange,
							 String receivedRoutingKey, Boolean redelivered, long deliveryTag,
							 int messageCount) {
		super();
		this.basicProperties = basicProperties;
		this.receivedExchange = receivedExchange;
		this.routingKey = receivedRoutingKey;
		this.redelivered = redelivered;
		this.deliveryTag = deliveryTag;
		this.messageCount = messageCount;
		initializeHeadersIfNecessary(basicProperties);
	}


	private void initializeHeadersIfNecessary(BasicProperties properties) {
		if (properties.getHeaders() == null) {
			properties.setHeaders(new HashMap<String, Object>());
		}
	}

	/**
	 * Return Rabbit BasicProperties.
	 * @return the Rabbit BasicProperties  
	 */
	public BasicProperties getBasicProperties() {
		return this.basicProperties;
	}

	public Map<String, Object> getHeaders() {
		return basicProperties.getHeaders();
	}

	public void setHeader(String key, Object value) {
		basicProperties.getHeaders().put(key, value);
	}

	public Date getTimestamp() {
		return this.basicProperties.getTimestamp();
	}

	public void setAppId(String appId) {
		basicProperties.setAppId(appId);
	}

	public String getAppId() {
		return basicProperties.getAppId();
	}

	public void setUserId(String userId) {
		basicProperties.setUserId(userId);
	}

	public String getUserId() {
		return basicProperties.getUserId();
	}
	
	public void setType(String type) {
		basicProperties.setType(type);
	}
	
	public String getType() {
		return basicProperties.getType();
	}
	
	public void setMessageId(String id) {
		basicProperties.setMessageId(id);
	}
	
	public String getMessageId() {
		return basicProperties.getMessageId();
	}
	
	public void setClusterId(String id) {
		basicProperties.setClusterId(id);
	}
	
	public String getClusterId() {
		return basicProperties.getClusterId();
	}

	public void setCorrelationId(byte[] correlationId) {
		//TODO this isn't exactly correct...
		try {
			if (correlationId == null) {
				basicProperties.setCorrelationId(null);
			}
			else {
				basicProperties.setCorrelationId(new String(correlationId, this.defaultCharset));
			}
		}
		catch (UnsupportedEncodingException e) {
			throw new AmqpUnsupportedEncodingException(e);
		}
	}

	public byte[] getCorrelationId() {
		if (basicProperties.getCorrelationId() == null) {
			return null;
		}
		try {
			return basicProperties.getCorrelationId().getBytes(this.defaultCharset);
		}
		catch (UnsupportedEncodingException ex) {
			throw new AmqpUnsupportedEncodingException(ex);
		}
	}

	public void setReplyTo(Address replyTo) {
		Assert.notNull(replyTo, "replyTo must not be null");
		basicProperties.setReplyTo(replyTo.toString());
	}

	public Address getReplyTo() {
		String replyTo = basicProperties.getReplyTo();
		return (replyTo != null) ? new Address(replyTo) : null;
	}

	public void setContentType(String contentType) {
		basicProperties.setContentType(contentType);
	}

	public String getContentType() {
		return basicProperties.getContentType();
	}

	public void setContentEncoding(String contentEncoding) {
		basicProperties.setContentEncoding(contentEncoding);
	}

	public String getContentEncoding() {
		return basicProperties.getContentEncoding();
	}

	public void setContentLength(long contentLength) {
		this.contentLength = contentLength;
	}

	public long getContentLength() {
		return this.contentLength;
	}

	public void setDefaultCharset(String charSet) {
		this.defaultCharset = (defaultCharset != null) ? defaultCharset : DEFAULT_CHARSET;
	}

	public void setDeliveryMode(MessageDeliveryMode deliveryMode) {
		Assert.notNull(deliveryMode, "deliveryMode must not be null");
		this.basicProperties.setDeliveryMode(MessageDeliveryMode.toInt(deliveryMode));
	}

	public MessageDeliveryMode getDeliveryMode() {
		Integer deliveryMode = this.basicProperties.getDeliveryMode();
		return (deliveryMode != null) ? MessageDeliveryMode.fromInt(deliveryMode) : null;
	}

	public void setExpiration(String expiration) {
		this.basicProperties.setExpiration(expiration);
	}

	public String getExpiration() {
		return this.basicProperties.getExpiration();
	}

	public void setPriority(Integer priority) {
		this.basicProperties.setPriority(priority);
	}

	public Integer getPriority() {
		return this.basicProperties.getPriority();
	}

	public String getReceivedExchange() {
		return this.receivedExchange;
	}

	public String getReceivedRoutingKey() {
		return this.routingKey;
	}

	public Boolean isRedelivered() {
		return this.redelivered;
	}

	public long getDeliveryTag() {
		return this.deliveryTag;
	}

	public Integer getMessageCount() {
		return this.messageCount;
	}

}
