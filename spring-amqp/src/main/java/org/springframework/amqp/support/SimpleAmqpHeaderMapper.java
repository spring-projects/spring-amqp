/*
 * Copyright 2002-2016 the original author or authors.
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
import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.AbstractHeaderMapper;
import org.springframework.util.StringUtils;

/**
 * Simple implementation of {@link AmqpHeaderMapper}.
 *
 * <p>This implementation copies AMQP API headers (e.g. appId) to and from
 * {@link MessageHeaders}.Any used-defined properties will also be copied
 * from an AMQP Message to a {@link org.springframework.amqp.core.Message
 * Message}, and any other headers on a Message (beyond the AMQP headers)
 * will likewise be copied to an AMQP Message. Those other headers will be
 * copied to the general headers of a {@link MessageProperties} whereas the
 * AMQP API headers are passed to the appropriate setter methods (e.g.
 * {@link MessageProperties#setAppId}.
 *
 * <p>Constants for the AMQP header keys are defined in {@link AmqpHeaders}.
 *
 * @author Mark Fisher
 * @author Oleg Zhurakousky
 * @author Gary Russell
 * @author Artem Bilan
 * @author Stephane Nicoll
 * @since 1.4
 */
public class SimpleAmqpHeaderMapper extends AbstractHeaderMapper<MessageProperties> implements AmqpHeaderMapper {

	@Override
	public void fromHeaders(MessageHeaders headers, MessageProperties amqpMessageProperties) {
		String appId = getHeaderIfAvailable(headers, AmqpHeaders.APP_ID, String.class);
		if (StringUtils.hasText(appId)) {
			amqpMessageProperties.setAppId(appId);
		}
		String clusterId = getHeaderIfAvailable(headers, AmqpHeaders.CLUSTER_ID, String.class);
		if (StringUtils.hasText(clusterId)) {
			amqpMessageProperties.setClusterId(clusterId);
		}
		String contentEncoding = getHeaderIfAvailable(headers, AmqpHeaders.CONTENT_ENCODING, String.class);
		if (StringUtils.hasText(contentEncoding)) {
			amqpMessageProperties.setContentEncoding(contentEncoding);
		}
		Long contentLength = getHeaderIfAvailable(headers, AmqpHeaders.CONTENT_LENGTH, Long.class);
		if (contentLength != null) {
			amqpMessageProperties.setContentLength(contentLength);
		}
		String contentType = this.extractContentTypeAsString(headers);

		if (StringUtils.hasText(contentType)) {
			amqpMessageProperties.setContentType(contentType);
		}
		Object correlationId = headers.get(AmqpHeaders.CORRELATION_ID);
		if (correlationId instanceof byte[]) {
			amqpMessageProperties.setCorrelationId((byte[]) correlationId);
		}
		Integer delay = getHeaderIfAvailable(headers, AmqpHeaders.DELAY, Integer.class);
		if (delay != null) {
			amqpMessageProperties.setDelay(delay);
		}
		MessageDeliveryMode deliveryMode = getHeaderIfAvailable(headers, AmqpHeaders.DELIVERY_MODE, MessageDeliveryMode.class);
		if (deliveryMode != null) {
			amqpMessageProperties.setDeliveryMode(deliveryMode);
		}
		Long deliveryTag = getHeaderIfAvailable(headers, AmqpHeaders.DELIVERY_TAG, Long.class);
		if (deliveryTag != null) {
			amqpMessageProperties.setDeliveryTag(deliveryTag);
		}
		String expiration = getHeaderIfAvailable(headers, AmqpHeaders.EXPIRATION, String.class);
		if (StringUtils.hasText(expiration)) {
			amqpMessageProperties.setExpiration(expiration);
		}
		Integer messageCount = getHeaderIfAvailable(headers, AmqpHeaders.MESSAGE_COUNT, Integer.class);
		if (messageCount != null) {
			amqpMessageProperties.setMessageCount(messageCount);
		}
		String messageId = getHeaderIfAvailable(headers, AmqpHeaders.MESSAGE_ID, String.class);
		if (StringUtils.hasText(messageId)) {
			amqpMessageProperties.setMessageId(messageId);
		}
		Integer priority = getHeaderIfAvailable(headers, AmqpMessageHeaderAccessor.PRIORITY, Integer.class);
		if (priority != null) {
			amqpMessageProperties.setPriority(priority);
		}
		String receivedExchange = getHeaderIfAvailable(headers, AmqpHeaders.RECEIVED_EXCHANGE, String.class);
		if (StringUtils.hasText(receivedExchange)) {
			amqpMessageProperties.setReceivedExchange(receivedExchange);
		}
		String receivedRoutingKey = getHeaderIfAvailable(headers, AmqpHeaders.RECEIVED_ROUTING_KEY, String.class);
		if (StringUtils.hasText(receivedRoutingKey)) {
			amqpMessageProperties.setReceivedRoutingKey(receivedRoutingKey);
		}
		Boolean redelivered = getHeaderIfAvailable(headers, AmqpHeaders.REDELIVERED, Boolean.class);
		if (redelivered != null) {
			amqpMessageProperties.setRedelivered(redelivered);
		}
		String replyTo = getHeaderIfAvailable(headers, AmqpHeaders.REPLY_TO, String.class);
		if (replyTo != null) {
			amqpMessageProperties.setReplyTo(replyTo);
		}
		Date timestamp = getHeaderIfAvailable(headers, AmqpHeaders.TIMESTAMP, Date.class);
		if (timestamp != null) {
			amqpMessageProperties.setTimestamp(timestamp);
		}
		String type = getHeaderIfAvailable(headers, AmqpHeaders.TYPE, String.class);
		if (type != null) {
			amqpMessageProperties.setType(type);
		}
		String userId = getHeaderIfAvailable(headers, AmqpHeaders.USER_ID, String.class);
		if (StringUtils.hasText(userId)) {
			amqpMessageProperties.setUserId(userId);
		}

		String replyCorrelation = getHeaderIfAvailable(headers, AmqpHeaders.SPRING_REPLY_CORRELATION, String.class);
		if (StringUtils.hasLength(replyCorrelation)) {
			amqpMessageProperties.setHeader("spring_reply_correlation", replyCorrelation);
		}
		String replyToStack = getHeaderIfAvailable(headers, AmqpHeaders.SPRING_REPLY_TO_STACK, String.class);
		if (StringUtils.hasLength(replyToStack)) {
			amqpMessageProperties.setHeader("spring_reply_to", replyToStack);
		}

		// Map custom headers
		for (Map.Entry<String, Object> entry : headers.entrySet()) {
			String headerName = entry.getKey();
			if (StringUtils.hasText(headerName) && !headerName.startsWith(AmqpHeaders.PREFIX)) {
				Object value = entry.getValue();
				if (value != null) {
					String propertyName = this.fromHeaderName(headerName);
					if (!amqpMessageProperties.getHeaders().containsKey(headerName)) {
						amqpMessageProperties.setHeader(propertyName, value);
					}
				}
			}
		}
	}

	@Override
	public MessageHeaders toHeaders(MessageProperties amqpMessageProperties) {
		Map<String, Object> headers = new HashMap<String, Object>();
		try {
			String appId = amqpMessageProperties.getAppId();
			if (StringUtils.hasText(appId)) {
				headers.put(AmqpHeaders.APP_ID, appId);
			}
			String clusterId = amqpMessageProperties.getClusterId();
			if (StringUtils.hasText(clusterId)) {
				headers.put(AmqpHeaders.CLUSTER_ID, clusterId);
			}
			String contentEncoding = amqpMessageProperties.getContentEncoding();
			if (StringUtils.hasText(contentEncoding)) {
				headers.put(AmqpHeaders.CONTENT_ENCODING, contentEncoding);
			}
			long contentLength = amqpMessageProperties.getContentLength();
			if (contentLength > 0) {
				headers.put(AmqpHeaders.CONTENT_LENGTH, contentLength);
			}
			String contentType = amqpMessageProperties.getContentType();
			if (StringUtils.hasText(contentType)) {
				headers.put(AmqpHeaders.CONTENT_TYPE, contentType);
			}
			byte[] correlationId = amqpMessageProperties.getCorrelationId();
			if (correlationId != null && correlationId.length > 0) {
				headers.put(AmqpHeaders.CORRELATION_ID, correlationId);
			}
			MessageDeliveryMode deliveryMode = amqpMessageProperties.getDeliveryMode();
			if (deliveryMode != null) {
				headers.put(AmqpHeaders.DELIVERY_MODE, deliveryMode);
			}
			long deliveryTag = amqpMessageProperties.getDeliveryTag();
			if (deliveryTag > 0) {
				headers.put(AmqpHeaders.DELIVERY_TAG, deliveryTag);
			}
			String expiration = amqpMessageProperties.getExpiration();
			if (StringUtils.hasText(expiration)) {
				headers.put(AmqpHeaders.EXPIRATION, expiration);
			}
			Integer messageCount = amqpMessageProperties.getMessageCount();
			if (messageCount != null && messageCount > 0) {
				headers.put(AmqpHeaders.MESSAGE_COUNT, messageCount);
			}
			String messageId = amqpMessageProperties.getMessageId();
			if (StringUtils.hasText(messageId)) {
				headers.put(AmqpHeaders.MESSAGE_ID, messageId);
			}
			Integer priority = amqpMessageProperties.getPriority();
			if (priority != null && priority > 0) {
				headers.put(AmqpMessageHeaderAccessor.PRIORITY, priority);
			}
			Integer receivedDelay = amqpMessageProperties.getReceivedDelay();
			if (receivedDelay != null) {
				headers.put(AmqpHeaders.RECEIVED_DELAY, receivedDelay);
			}
			String receivedExchange = amqpMessageProperties.getReceivedExchange();
			if (StringUtils.hasText(receivedExchange)) {
				headers.put(AmqpHeaders.RECEIVED_EXCHANGE, receivedExchange);
			}
			String receivedRoutingKey = amqpMessageProperties.getReceivedRoutingKey();
			if (StringUtils.hasText(receivedRoutingKey)) {
				headers.put(AmqpHeaders.RECEIVED_ROUTING_KEY, receivedRoutingKey);
			}
			Boolean redelivered = amqpMessageProperties.isRedelivered();
			if (redelivered != null) {
				headers.put(AmqpHeaders.REDELIVERED, redelivered);
			}
			String replyTo = amqpMessageProperties.getReplyTo();
			if (replyTo != null) {
				headers.put(AmqpHeaders.REPLY_TO, replyTo);
			}
			Date timestamp = amqpMessageProperties.getTimestamp();
			if (timestamp != null) {
				headers.put(AmqpHeaders.TIMESTAMP, timestamp);
			}
			String type = amqpMessageProperties.getType();
			if (StringUtils.hasText(type)) {
				headers.put(AmqpHeaders.TYPE, type);
			}
			String userId = amqpMessageProperties.getUserId();
			if (StringUtils.hasText(userId)) {
				headers.put(AmqpHeaders.USER_ID, userId);
			}
			String consumerTag = amqpMessageProperties.getConsumerTag();
			if (StringUtils.hasText(consumerTag)) {
				headers.put(AmqpHeaders.CONSUMER_TAG, consumerTag);
			}
			String consumerQueue = amqpMessageProperties.getConsumerQueue();
			if (StringUtils.hasText(consumerQueue)) {
				headers.put(AmqpHeaders.CONSUMER_QUEUE, consumerQueue);
			}

			// Map custom headers
			for (Map.Entry<String, Object> entry : amqpMessageProperties.getHeaders().entrySet()) {
				headers.put(entry.getKey(), entry.getValue());
			}
		}
		catch (Exception e) {
			if (logger.isWarnEnabled()) {
				logger.warn("error occurred while mapping from AMQP properties to MessageHeaders", e);
			}
		}
		return new MessageHeaders(headers);
	}

	/**
	 * Will extract Content-Type from MessageHeaders and convert it to String if possible
	 * Required since Content-Type can be represented as org.springframework.http.MediaType
	 * or org.springframework.util.MimeType.
	 */
	private String extractContentTypeAsString(Map<String, Object> headers) {
		String contentTypeStringValue = null;

		Object contentType = getHeaderIfAvailable(headers, AmqpHeaders.CONTENT_TYPE, Object.class);

		if (contentType != null) {
			String contentTypeClassName = contentType.getClass().getName();

			// TODO: 2.0 - check instanceof MimeType instead
			if (contentTypeClassName.equals("org.springframework.http.MediaType")
					|| contentTypeClassName.equals("org.springframework.util.MimeType")) {
				contentTypeStringValue = contentType.toString();
			}
			else if (contentType instanceof String) {
				contentTypeStringValue = (String) contentType;
			}
			else {
				if (logger.isWarnEnabled()) {
					logger.warn("skipping header '" + AmqpHeaders.CONTENT_TYPE +
							"' since it is not of expected type [" + contentTypeClassName + "]");
				}
			}
		}
		return contentTypeStringValue;
	}

}
