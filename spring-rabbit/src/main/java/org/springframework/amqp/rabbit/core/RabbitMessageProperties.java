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
	public BasicProperties getBasicProperties() 
	{
		return this.basicProperties;
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getHeaders()
	 */
	public Map<String, Object> getHeaders() {
		return basicProperties.getHeaders();
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setHeader(java.lang.String, java.lang.Object)
	 */
	public void setHeader(String key, Object value) {
		basicProperties.getHeaders().put(key, value);
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getTimestamp()
	 */
	public Date getTimestamp() {
		return this.basicProperties.getTimestamp();
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setAppId(java.lang.String)
	 */
	public void setAppId(String appId) {
		basicProperties.setAppId(appId);
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getAppId()
	 */
	public String getAppId() {
		return basicProperties.getAppId();
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setUserId(java.lang.String)
	 */
	public void setUserId(String userId) {
		basicProperties.setUserId(userId);
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getUserId()
	 */
	public String getUserId() {
		return basicProperties.getUserId();
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setType(java.lang.String)
	 */
	public void setType(String type)
	{
		basicProperties.setType(type);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getType()
	 */
	public String getType() {
		return basicProperties.getType();
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setMessageId(java.lang.String)
	 */
	public void setMessageId(String id)
	{
		basicProperties.setMessageId(id);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getMessageId()
	 */
	public String getMessageId()
	{
		return basicProperties.getMessageId();
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setClusterId(java.lang.String)
	 */
	public void setClusterId(String id)
	{
		basicProperties.setClusterId(id);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getClusterId()
	 */
	public String getClusterId()
	{
		return basicProperties.getClusterId();
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setCorrelationId(byte[])
	 */
	public void setCorrelationId(byte[] correlationId) {
		//TODO this isn't exactly correct...
		try {
			basicProperties.setCorrelationId(new String(correlationId, this.defaultCharset));
		}
		catch (UnsupportedEncodingException e) {
			throw new AmqpUnsupportedEncodingException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getCorrelationId()
	 */
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

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setReplyTo(java.lang.String)
	 */
	public void setReplyTo(Address replyTo) {
		basicProperties.setReplyTo(replyTo.toString());
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getReplyTo()
	 */
	public Address getReplyTo() {
		return Address.parse(basicProperties.getReplyTo());
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setContentType(java.lang.String)
	 */
	public void setContentType(String contentType) {
		basicProperties.setContentType(contentType);
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getContentType()
	 */
	public String getContentType() {
		return basicProperties.getContentType();
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setContentEncoding(java.lang.String)
	 */
	public void setContentEncoding(String contentEncoding) {
		basicProperties.setContentEncoding(contentEncoding);
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getContentEncoding()
	 */
	public String getContentEncoding() {
		return basicProperties.getContentEncoding();
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setContentLength(long)
	 */
	public void setContentLength(long contentLength) {
		this.contentLength = contentLength;
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getContentLength()
	 */
	public long getContentLength() {
		
		return this.contentLength;
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setDefaultCharset(java.lang.String)
	 */
	public void setDefaultCharset(String charSet) {
		this.defaultCharset = (defaultCharset != null) ? defaultCharset : DEFAULT_CHARSET;
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setDeliveryMode(java.lang.Integer)
	 */
	public void setDeliveryMode(MessageDeliveryMode deliveryMode) {
		
		this.basicProperties.setDeliveryMode(MessageDeliveryMode.toInt(deliveryMode));
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getDeliveryMode()
	 */
	public MessageDeliveryMode getDeliveryMode() {
		int deliveryMode = this.basicProperties.getDeliveryMode().intValue();
		return MessageDeliveryMode.fromInt(deliveryMode);
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setExpiration(java.lang.String)
	 */
	public void setExpiration(String expiration) {
		this.basicProperties.setExpiration(expiration);
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getExpiration()
	 */
	public String getExpiration() {
		return this.basicProperties.getExpiration();
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#setPriority(java.lang.Integer)
	 */
	public void setPriority(Integer priority) {
		this.basicProperties.setPriority(priority);
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getPriority()
	 */
	public Integer getPriority() {
		return this.basicProperties.getPriority();
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getReceivedExchange()
	 */
	public String getReceivedExchange() {
		return this.receivedExchange;
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getReceivedRoutingKey()
	 */
	public String getReceivedRoutingKey() {
		return this.routingKey;
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#isRedelivered()
	 */
	public Boolean isRedelivered() {
		return this.redelivered;
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getDeliveryTag()
	 */
	public long getDeliveryTag() {
		return this.deliveryTag;
	}

	/* (non-Javadoc)
	 * @see org.springframework.amqp.core.MessageProperties#getMessageCount()
	 */
	public Integer getMessageCount() {
		return this.messageCount;
	}

}
