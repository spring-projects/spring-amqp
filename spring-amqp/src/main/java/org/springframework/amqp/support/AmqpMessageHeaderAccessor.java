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

package org.springframework.amqp.support;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.NativeMessageHeaderAccessor;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;

/**
 * A {@link org.springframework.messaging.support.MessageHeaderAccessor}
 * implementation giving access to AMQP-specific headers.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @since 1.4
 */
public class AmqpMessageHeaderAccessor extends NativeMessageHeaderAccessor {

	public static final String PRIORITY = "priority";

	protected AmqpMessageHeaderAccessor(Map<String, List<String>> nativeHeaders) {
		super(nativeHeaders);
	}

	protected AmqpMessageHeaderAccessor(Message<?> message) {
		super(message);
	}

	// Static factory method

	/**
	 * Create a {@link AmqpMessageHeaderAccessor} from the headers of an existing message.
	 * @param message the {@link Message} to be wrapped for this
	 * {@link org.springframework.messaging.support.MessageHeaderAccessor}.
	 * @return the {@link AmqpMessageHeaderAccessor} instance.
	 */
	public static AmqpMessageHeaderAccessor wrap(Message<?> message) {
		return new AmqpMessageHeaderAccessor(message);
	}

	@Override
	protected void verifyType(String headerName, Object headerValue) {
		super.verifyType(headerName, headerValue);
		if (PRIORITY.equals(headerName)) {
			Assert.isTrue(Integer.class.isAssignableFrom(headerValue.getClass()), "The '" + headerName
					+ "' header value must be an Integer.");
		}
	}

	public String getAppId() {
		return (String) getHeader(AmqpHeaders.APP_ID);
	}

	public String getClusterId() {
		return (String) getHeader(AmqpHeaders.CLUSTER_ID);
	}

	public String getContentEncoding() {
		return (String) getHeader(AmqpHeaders.CONTENT_ENCODING);
	}

	public Long getContentLength() {
		return (Long) getHeader(AmqpHeaders.CONTENT_LENGTH);
	}

	@Override
	public MimeType getContentType() {
		Object value = getHeader(AmqpHeaders.CONTENT_TYPE);
		if (value != null && value instanceof String) {
			return MimeType.valueOf((String) value);
		}
		else {
			return super.getContentType();
		}
	}

	public byte[] getCorrelationId() {
		return (byte[]) getHeader(AmqpHeaders.CORRELATION_ID);
	}

	public MessageDeliveryMode getDeliveryMode() {
		return (MessageDeliveryMode) getHeader(AmqpHeaders.DELIVERY_MODE);
	}

	public Long getDeliveryTag() {
		return (Long) getHeader(AmqpHeaders.DELIVERY_TAG);
	}

	public String getExpiration() {
		return (String) getHeader(AmqpHeaders.EXPIRATION);
	}

	public Integer getMessageCount() {
		return (Integer) getHeader(AmqpHeaders.MESSAGE_COUNT);
	}

	public String getMessageId() {
		return (String) getHeader(AmqpHeaders.MESSAGE_ID);
	}

	public Integer getPriority() {
		return (Integer) getHeader(PRIORITY);
	}

	public String getReceivedExchange() {
		return (String) getHeader(AmqpHeaders.RECEIVED_EXCHANGE);
	}

	public String getReceivedRoutingKey() {
		return (String) getHeader(AmqpHeaders.RECEIVED_ROUTING_KEY);
	}

	public Boolean getRedelivered() {
		return (Boolean) getHeader(AmqpHeaders.REDELIVERED);
	}

	public String getReplyTo() {
		return (String) getHeader(AmqpHeaders.REPLY_TO);
	}

	@Override
	public Long getTimestamp() {
		Date amqpTimestamp = (Date) getHeader(AmqpHeaders.TIMESTAMP);
		if (amqpTimestamp != null) {
			return amqpTimestamp.getTime();
		}
		else {
			return super.getTimestamp();
		}
	}

	public String getType() {
		return (String) getHeader(AmqpHeaders.TYPE);
	}

	public String getUserId() {
		return (String) getHeader(AmqpHeaders.USER_ID);
	}

	public String getConsumerTag() {
		return (String) getHeader(AmqpHeaders.CONSUMER_TAG);
	}

	public String getConsumerQueue() {
		return (String) getHeader(AmqpHeaders.CONSUMER_QUEUE);
	}

}
