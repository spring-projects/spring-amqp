/*
 * Copyright 2002-2022 the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;

/**
 * Default implementation of the {@link MessagePropertiesConverter} strategy.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Soeren Unruh
 * @since 1.0
 */
public class DefaultMessagePropertiesConverter implements MessagePropertiesConverter {

	private static final int DEFAULT_LONG_STRING_LIMIT = 1024;

	private final int longStringLimit;

	private final boolean convertLongLongStrings;

	/**
	 * Construct an instance where {@link LongString}s will be returned
	 * unconverted when longer than 1024 bytes.
	 */
	public DefaultMessagePropertiesConverter() {
		this(DEFAULT_LONG_STRING_LIMIT, false);
	}

	/**
	 * Construct an instance where {@link LongString}s will be returned
	 * unconverted when longer than this limit.
	 * @param longStringLimit the limit.
	 * @since 1.4.4
	 */
	public DefaultMessagePropertiesConverter(int longStringLimit) {
		this(longStringLimit, false);
	}

	/**
	 * Construct an instance where {@link LongString}s will be returned
	 * unconverted or as a {@link java.io.DataInputStream} when longer than this limit.
	 * Use this constructor with 'true' to restore pre-1.6 behavior.
	 * @param longStringLimit the limit.
	 * @param convertLongLongStrings {@link LongString} when false,
	 * {@link java.io.DataInputStream} when true.
	 * @since 1.6
	 */
	public DefaultMessagePropertiesConverter(int longStringLimit, boolean convertLongLongStrings) {
		this.longStringLimit = longStringLimit;
		this.convertLongLongStrings = convertLongLongStrings;
	}

	@Override
	public MessageProperties toMessageProperties(final BasicProperties source, final Envelope envelope,
			final String charset) {
		MessageProperties target = new MessageProperties();
		Map<String, Object> headers = source.getHeaders();
		if (!CollectionUtils.isEmpty(headers)) {
			for (Map.Entry<String, Object> entry : headers.entrySet()) {
				String key = entry.getKey();
				if (MessageProperties.X_DELAY.equals(key)) {
					Object value = entry.getValue();
					if (value instanceof Integer integ) {
						target.setReceivedDelay(integ);
					}
				}
				else {
					target.setHeader(key, convertLongStringIfNecessary(entry.getValue(), charset));
				}
			}
		}
		target.setTimestamp(source.getTimestamp());
		target.setMessageId(source.getMessageId());
		target.setReceivedUserId(source.getUserId());
		target.setAppId(source.getAppId());
		target.setClusterId(source.getClusterId());
		target.setType(source.getType());
		Integer deliveryMode = source.getDeliveryMode();
		if (deliveryMode != null) {
			target.setReceivedDeliveryMode(MessageDeliveryMode.fromInt(deliveryMode));
		}
		target.setDeliveryMode(null);
		target.setExpiration(source.getExpiration());
		target.setPriority(source.getPriority());
		target.setContentType(source.getContentType());
		target.setContentEncoding(source.getContentEncoding());
		String correlationId = source.getCorrelationId();
		if (StringUtils.hasText(correlationId)) {
			target.setCorrelationId(source.getCorrelationId());
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

	@Override
	public BasicProperties fromMessageProperties(final MessageProperties source, final String charset) {
		BasicProperties.Builder target = new BasicProperties.Builder();
		target.headers(this.convertHeadersIfNecessary(source.getHeaders()))
			.timestamp(source.getTimestamp())
			.messageId(source.getMessageId())
			.userId(source.getUserId())
			.appId(source.getAppId())
			.clusterId(source.getClusterId())
			.type(source.getType());
		MessageDeliveryMode deliveryMode = source.getDeliveryMode();
		if (deliveryMode != null) {
			target.deliveryMode(MessageDeliveryMode.toInt(deliveryMode));
		}
		target.expiration(source.getExpiration())
			.priority(source.getPriority())
			.contentType(source.getContentType())
			.contentEncoding(source.getContentEncoding());
		String correlationId = source.getCorrelationId();
		if (StringUtils.hasText(correlationId)) {
			target.correlationId(correlationId);
		}
		String replyTo = source.getReplyTo();
		if (replyTo != null) {
			target.replyTo(replyTo);
		}
		return target.build();
	}

	private Map<String, Object> convertHeadersIfNecessary(Map<String, Object> headers) {
		if (CollectionUtils.isEmpty(headers)) {
			return Collections.<String, Object>emptyMap();
		}
		Map<String, Object> writableHeaders = new HashMap<String, Object>();
		for (Map.Entry<String, Object> entry : headers.entrySet()) {
			writableHeaders.put(entry.getKey(), this.convertHeaderValueIfNecessary(entry.getValue()));
		}
		return writableHeaders;
	}

	/**
	 * Converts a header value to a String if the value type is unsupported by AMQP, also handling values
	 * nested inside Lists or Maps.
	 * <p> {@code null} values are passed through, although Rabbit client will throw an IllegalArgumentException.
	 * @param valueArg the value.
	 * @return the converted value.
	 */
	@Nullable // NOSONAR complexity
	private Object convertHeaderValueIfNecessary(@Nullable Object valueArg) {
		Object value = valueArg;
		boolean valid = (value instanceof String) || (value instanceof byte[]) // NOSONAR boolean complexity
				|| (value instanceof Boolean) || (value instanceof Class)
				|| (value instanceof LongString) || (value instanceof Integer) || (value instanceof Long)
				|| (value instanceof Float) || (value instanceof Double) || (value instanceof BigDecimal)
				|| (value instanceof Short) || (value instanceof Byte) || (value instanceof Date)
				|| (value instanceof List) || (value instanceof Map) || (value instanceof Object[]);
		if (!valid && value != null) {
			value = value.toString();
		}
		else if (value instanceof Object[] array) {
			Object[] writableArray = new Object[array.length];
			for (int i = 0; i < writableArray.length; i++) {
				writableArray[i] = convertHeaderValueIfNecessary(array[i]);
			}
			value = writableArray;
		}
		else if (value instanceof List<?>) {
			List<Object> writableList = new ArrayList<Object>(((List<?>) value).size());
			for (Object listValue : (List<?>) value) {
				writableList.add(convertHeaderValueIfNecessary(listValue));
			}
			value = writableList;
		}
		else if (value instanceof Map<?, ?>) {
			@SuppressWarnings("unchecked")
			Map<String, Object> originalMap = (Map<String, Object>) value;
			Map<String, Object> writableMap = new HashMap<String, Object>(originalMap.size());
			for (Map.Entry<String, Object> entry : originalMap.entrySet()) {
				writableMap.put(entry.getKey(), this.convertHeaderValueIfNecessary(entry.getValue()));
			}
			value = writableMap;
		}
		else if (value instanceof Class<?> clazz) {
			value = clazz.getName();
		}
		return value;
	}

	/**
	 * Converts a LongString value to either a String or DataInputStream based on a
	 * length-driven threshold. If the length is {@link #longStringLimit} bytes or less, a
	 * String will be returned, otherwise a DataInputStream is returned or the {@link LongString}
	 * is returned unconverted if {@link #convertLongLongStrings} is true.
	 * @param longString the long string.
	 * @param charset the charset.
	 * @return the converted string.
	 */
	private Object convertLongString(LongString longString, String charset) {
		try {
			if (longString.length() <= this.longStringLimit) {
				return new String(longString.getBytes(), charset);
			}
			else {
				return this.convertLongLongStrings ? longString.getStream() : longString;
			}
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	/**
	 * Converts a LongString value using {@link #convertLongString(LongString, String)}, also handling values
	 * nested in Lists or Maps.
	 * @param valueArg the value.
	 * @param charset the charset.
	 * @return the converted string.
	 */
	private Object convertLongStringIfNecessary(Object valueArg, String charset) {
		Object value = valueArg;
		if (value instanceof LongString longStr) {
			value = convertLongString(longStr, charset);
		}
		else if (value instanceof List<?>) {
			List<Object> convertedList = new ArrayList<Object>(((List<?>) value).size());
			for (Object listValue : (List<?>) value) {
				convertedList.add(this.convertLongStringIfNecessary(listValue, charset));
			}
			value = convertedList;
		}
		else if (value instanceof Map<?, ?>) {
			@SuppressWarnings("unchecked")
			Map<String, Object> originalMap = (Map<String, Object>) value;
			Map<String, Object> convertedMap = new HashMap<String, Object>();
			for (Map.Entry<String, Object> entry : originalMap.entrySet()) {
				convertedMap.put(entry.getKey(), this.convertLongStringIfNecessary(entry.getValue(), charset));
			}
			value = convertedMap;
		}
		return value;
	}

}
