/*
 * Copyright 2002-2019 the original author or authors.
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
import java.util.function.BiConsumer;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.AbstractHeaderMapper;
import org.springframework.util.MimeType;
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
		JavaUtils javaUtils = JavaUtils.INSTANCE
			.acceptIfHasText(getHeaderIfAvailable(headers, AmqpHeaders.APP_ID, String.class),
					amqpMessageProperties::setAppId)
			.acceptIfHasText(getHeaderIfAvailable(headers, AmqpHeaders.CLUSTER_ID, String.class),
					amqpMessageProperties::setClusterId)
			.acceptIfHasText(getHeaderIfAvailable(headers, AmqpHeaders.CONTENT_ENCODING, String.class),
					amqpMessageProperties::setContentEncoding)
			.acceptIfNotNull(getHeaderIfAvailable(headers, AmqpHeaders.CONTENT_LENGTH, Long.class),
					amqpMessageProperties::setContentLength)
			.acceptIfHasText(extractContentTypeAsString(headers), amqpMessageProperties::setContentType);
		Object correlationId = headers.get(AmqpHeaders.CORRELATION_ID);
		if (correlationId instanceof String) {
			amqpMessageProperties.setCorrelationId((String) correlationId);
		}
		javaUtils
			.acceptIfNotNull(getHeaderIfAvailable(headers, AmqpHeaders.DELAY, Integer.class),
					amqpMessageProperties::setDelay)
			.acceptIfNotNull(getHeaderIfAvailable(headers, AmqpHeaders.DELIVERY_MODE, MessageDeliveryMode.class),
					amqpMessageProperties::setDeliveryMode)
			.acceptIfNotNull(getHeaderIfAvailable(headers, AmqpHeaders.DELIVERY_TAG, Long.class),
					amqpMessageProperties::setDeliveryTag)
			.acceptIfHasText(getHeaderIfAvailable(headers, AmqpHeaders.EXPIRATION, String.class),
					amqpMessageProperties::setExpiration)
			.acceptIfNotNull(getHeaderIfAvailable(headers, AmqpHeaders.MESSAGE_COUNT, Integer.class),
					amqpMessageProperties::setMessageCount)
			.acceptIfHasText(getHeaderIfAvailable(headers, AmqpHeaders.MESSAGE_ID, String.class),
					amqpMessageProperties::setMessageId)
			.acceptIfNotNull(getHeaderIfAvailable(headers, AmqpMessageHeaderAccessor.PRIORITY, Integer.class),
					amqpMessageProperties::setPriority)
			.acceptIfHasText(getHeaderIfAvailable(headers, AmqpHeaders.RECEIVED_EXCHANGE, String.class),
					amqpMessageProperties::setReceivedExchange)
			.acceptIfHasText(getHeaderIfAvailable(headers, AmqpHeaders.RECEIVED_ROUTING_KEY, String.class),
					amqpMessageProperties::setReceivedRoutingKey)
			.acceptIfNotNull(getHeaderIfAvailable(headers, AmqpHeaders.REDELIVERED, Boolean.class),
					amqpMessageProperties::setRedelivered)
			.acceptIfNotNull(getHeaderIfAvailable(headers, AmqpHeaders.REPLY_TO, String.class),
					amqpMessageProperties::setReplyTo)
			.acceptIfNotNull(getHeaderIfAvailable(headers, AmqpHeaders.TIMESTAMP, Date.class),
					amqpMessageProperties::setTimestamp)
			.acceptIfNotNull(getHeaderIfAvailable(headers, AmqpHeaders.TYPE, String.class),
					amqpMessageProperties::setType)
			.acceptIfHasText(getHeaderIfAvailable(headers, AmqpHeaders.USER_ID, String.class),
					amqpMessageProperties::setUserId);

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
			BiConsumer<String, Object> putObject = headers::put;
			BiConsumer<String, String> putString = headers::put;
			JavaUtils javaUtils = JavaUtils.INSTANCE
					.acceptIfNotNull(amqpMessageProperties.getAppId(), AmqpHeaders.APP_ID, putObject)
					.acceptIfNotNull(amqpMessageProperties.getClusterId(), AmqpHeaders.CLUSTER_ID, putObject)
					.acceptIfNotNull(amqpMessageProperties.getContentEncoding(), AmqpHeaders.CONTENT_ENCODING,
							putObject);
			long contentLength = amqpMessageProperties.getContentLength();
			javaUtils
					.acceptIfCondition(contentLength > 0, contentLength, AmqpHeaders.CONTENT_LENGTH, putObject)
					.acceptIfHasText(amqpMessageProperties.getContentType(), AmqpHeaders.CONTENT_TYPE, putString)
					.acceptIfHasText(amqpMessageProperties.getCorrelationId(), AmqpHeaders.CORRELATION_ID, putString)
					.acceptIfNotNull(amqpMessageProperties.getReceivedDeliveryMode(),
							AmqpHeaders.RECEIVED_DELIVERY_MODE, putObject);
			long deliveryTag = amqpMessageProperties.getDeliveryTag();
			javaUtils
					.acceptIfCondition(deliveryTag > 0, deliveryTag, AmqpHeaders.DELIVERY_TAG, putObject)
					.acceptIfHasText(amqpMessageProperties.getExpiration(), AmqpHeaders.EXPIRATION, putString)
					.acceptIfNotNull(amqpMessageProperties.getMessageCount(), AmqpHeaders.MESSAGE_COUNT, putObject)
					.acceptIfNotNull(amqpMessageProperties.getMessageId(), AmqpHeaders.MESSAGE_ID, putObject);
			Integer priority = amqpMessageProperties.getPriority();
			javaUtils
					.acceptIfCondition(priority != null && priority > 0, priority, AmqpMessageHeaderAccessor.PRIORITY,
							putObject)
					.acceptIfNotNull(amqpMessageProperties.getReceivedDelay(), AmqpHeaders.RECEIVED_DELAY, putObject)
					.acceptIfHasText(amqpMessageProperties.getReceivedExchange(), AmqpHeaders.RECEIVED_EXCHANGE,
							putString)
					.acceptIfHasText(amqpMessageProperties.getReceivedRoutingKey(), AmqpHeaders.RECEIVED_ROUTING_KEY,
							putString)
					.acceptIfNotNull(amqpMessageProperties.isRedelivered(), AmqpHeaders.REDELIVERED, putObject)
					.acceptIfNotNull(amqpMessageProperties.getReplyTo(), AmqpHeaders.REPLY_TO, putObject)
					.acceptIfNotNull(amqpMessageProperties.getTimestamp(), AmqpHeaders.TIMESTAMP, putObject)
					.acceptIfHasText(amqpMessageProperties.getType(), AmqpHeaders.TYPE, putString)
					.acceptIfHasText(amqpMessageProperties.getReceivedUserId(), AmqpHeaders.RECEIVED_USER_ID,
							putString)
					.acceptIfHasText(amqpMessageProperties.getConsumerTag(), AmqpHeaders.CONSUMER_TAG, putString)
					.acceptIfHasText(amqpMessageProperties.getConsumerQueue(), AmqpHeaders.CONSUMER_QUEUE, putString);

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
	 * @param headers the headers.
	 * @return the content type.
	 */
	private String extractContentTypeAsString(Map<String, Object> headers) {
		String contentTypeStringValue = null;

		Object contentType = getHeaderIfAvailable(headers, AmqpHeaders.CONTENT_TYPE, Object.class);

		if (contentType != null) {
			if (contentType instanceof MimeType) {
				contentTypeStringValue = contentType.toString();
			}
			else if (contentType instanceof String) {
				contentTypeStringValue = (String) contentType;
			}
			else {
				if (logger.isWarnEnabled()) {
					logger.warn("skipping header '" + AmqpHeaders.CONTENT_TYPE +
							"' since it is not of expected type [" + contentType.getClass().getName() + "]");
				}
			}
		}
		return contentTypeStringValue;
	}

}
