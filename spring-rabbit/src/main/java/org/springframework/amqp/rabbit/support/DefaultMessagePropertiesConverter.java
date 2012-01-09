/*
 * Copyright 2002-2011 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.support;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.amqp.AmqpUnsupportedEncodingException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.util.CollectionUtils;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;

/**
 * Default implementation of the {@link MessagePropertiesConverter} strategy.
 * 
 * @author Mark Fisher
 * @since 1.0
 */
public class DefaultMessagePropertiesConverter implements MessagePropertiesConverter {

	public MessageProperties toMessageProperties(final BasicProperties source, final Envelope envelope,
			final String charset) {
		MessageProperties target = new MessageProperties();
		Map<String, Object> headers = source.getHeaders();
		if (!CollectionUtils.isEmpty(headers)) {
			for (Map.Entry<String, Object> entry : headers.entrySet()) {
				Object value = entry.getValue();
				if (value instanceof LongString) {
					value = this.convertLongString((LongString) value, charset);
				}
				target.setHeader(entry.getKey(), value);
			}
		}
		target.setTimestamp(source.getTimestamp());
		target.setMessageId(source.getMessageId());
		target.setUserId(source.getUserId());
		target.setAppId(source.getAppId());
		target.setClusterId(source.getClusterId());
		target.setType(source.getType());
		Integer deliverMode = source.getDeliveryMode();
		if (deliverMode != null) {
			target.setDeliveryMode(MessageDeliveryMode.fromInt(deliverMode));
		}
		target.setExpiration(source.getExpiration());
		target.setPriority(source.getPriority());
		target.setContentType(source.getContentType());
		target.setContentEncoding(source.getContentEncoding());
		String correlationId = source.getCorrelationId();
		if (correlationId != null) {
			try {
				target.setCorrelationId(source.getCorrelationId().getBytes(charset));
			} catch (UnsupportedEncodingException ex) {
				throw new AmqpUnsupportedEncodingException(ex);
			}
		}
		String replyTo = source.getReplyTo();
		if (replyTo != null) {
			target.setReplyTo(replyTo);
		}
		if (envelope != null) {
			target.setReceivedExchange(envelope.getExchange());
			target.setReceivedRoutingKey(envelope.getRoutingKey());
			target.setRedelivered(envelope.isRedeliver());
			target.setDeliveryTag(envelope.getDeliveryTag());
		}
		return target;
	}

	public BasicProperties fromMessageProperties(final MessageProperties source, final String charset) {
		BasicProperties.Builder target = new BasicProperties.Builder();
		target.headers(this.convertHeadersIfNecessary(source.getHeaders()));
		target.timestamp(source.getTimestamp());
		target.messageId(source.getMessageId());
		target.userId(source.getUserId());
		target.appId(source.getAppId());
		target.clusterId(source.getClusterId());
		target.type(source.getType());
		MessageDeliveryMode deliveryMode = source.getDeliveryMode();
		if (deliveryMode != null) {
			target.deliveryMode(MessageDeliveryMode.toInt(deliveryMode));
		}
		target.expiration(source.getExpiration());
		target.priority(source.getPriority());
		target.contentType(source.getContentType());
		target.contentEncoding(source.getContentEncoding());
		byte[] correlationId = source.getCorrelationId();
		if (correlationId != null && correlationId.length > 0) {
			try {
				target.correlationId(new String(correlationId, charset));
			} catch (UnsupportedEncodingException ex) {
				throw new AmqpUnsupportedEncodingException(ex);
			}
		}
		String replyTo = source.getReplyTo();
		if (replyTo != null) {
			target.replyTo(replyTo);
		}
		return target.build();
	}

	private Map<String, Object> convertHeadersIfNecessary(Map<String, Object> headers) {
		if (CollectionUtils.isEmpty(headers)) {
			return Collections.<String, Object> emptyMap();
		}
		Map<String, Object> writableHeaders = new HashMap<String, Object>();
		for (Map.Entry<String, Object> entry : headers.entrySet()) {
			writableHeaders.put(entry.getKey(), this.convertHeaderValueIfNecessary(entry.getValue()));
		}
		return writableHeaders;
	}

	private Object convertHeaderValueIfNecessary(Object value) {
		boolean valid = (value instanceof String) || (value instanceof byte[]) || (value instanceof Boolean)
				|| (value instanceof LongString) || (value instanceof Integer) || (value instanceof Long)
				|| (value instanceof Float) || (value instanceof Double) || (value instanceof BigDecimal)
				|| (value instanceof Short) || (value instanceof Byte) || (value instanceof Date)
				|| (value instanceof List) || (value instanceof Map);
		if (!valid && value != null) {
			value = value.toString();
		}
		return value;
	}

	/**
	 * Converts a LongString value to either a String or DataInputStream based on a length-driven threshold. If the
	 * length is 1024 bytes or less, a String will be returned, otherwise a DataInputStream is returned.
	 */
	private Object convertLongString(LongString longString, String charset) {
		try {
			if (longString.length() <= 1024) {
				return new String(longString.getBytes(), charset);
			} else {
				return longString.getStream();
			}
		} catch (Exception e) {
			throw RabbitUtils.convertRabbitAccessException(e);
		}
	}

}
