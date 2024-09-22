/*
 * Copyright 2002-2024 the original author or authors.
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
 * @author Raylax Grey
 * @author Ngoc Nhan
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
		if (correlationId instanceof String string) {
			amqpMessageProperties.setCorrelationId(string);
		}
		javaUtils
			.acceptIfNotNull(getHeaderIfAvailable(headers, AmqpHeaders.DELAY, Long.class),
					amqpMessageProperties::setDelayLong)
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
		Map<String, Object> headers = new HashMap<>();
		try {
			BiConsumer<String, Object> putObject = headers::put;
			BiConsumer<String, String> putString = headers::put;
			JavaUtils javaUtils = JavaUtils.INSTANCE
					.acceptIfNotNull(AmqpHeaders.APP_ID, amqpMessageProperties.getAppId(), putObject)
					.acceptIfNotNull(AmqpHeaders.CLUSTER_ID, amqpMessageProperties.getClusterId(), putObject)
					.acceptIfNotNull(AmqpHeaders.CONTENT_ENCODING, amqpMessageProperties.getContentEncoding(),
							putObject);
			long contentLength = amqpMessageProperties.getContentLength();
			javaUtils
					.acceptIfCondition(contentLength > 0, AmqpHeaders.CONTENT_LENGTH, contentLength, putObject)
					.acceptIfHasText(AmqpHeaders.CONTENT_TYPE, amqpMessageProperties.getContentType(), putString)
					.acceptIfHasText(AmqpHeaders.CORRELATION_ID, amqpMessageProperties.getCorrelationId(), putString)
					.acceptIfNotNull(AmqpHeaders.RECEIVED_DELIVERY_MODE,
							amqpMessageProperties.getReceivedDeliveryMode(), putObject);
			long deliveryTag = amqpMessageProperties.getDeliveryTag();
			javaUtils
					.acceptIfCondition(deliveryTag > 0, AmqpHeaders.DELIVERY_TAG, deliveryTag, putObject)
					.acceptIfHasText(AmqpHeaders.EXPIRATION, amqpMessageProperties.getExpiration(), putString)
					.acceptIfNotNull(AmqpHeaders.MESSAGE_COUNT, amqpMessageProperties.getMessageCount(), putObject)
					.acceptIfNotNull(AmqpHeaders.MESSAGE_ID, amqpMessageProperties.getMessageId(), putObject);
			Integer priority = amqpMessageProperties.getPriority();
			javaUtils
					.acceptIfCondition(priority != null && priority > 0, AmqpMessageHeaderAccessor.PRIORITY, priority,
							putObject)
					.acceptIfNotNull(AmqpHeaders.RECEIVED_DELAY, amqpMessageProperties.getReceivedDelayLong(), putObject)
					.acceptIfHasText(AmqpHeaders.RECEIVED_EXCHANGE, amqpMessageProperties.getReceivedExchange(),
							putString)
					.acceptIfHasText(AmqpHeaders.RECEIVED_ROUTING_KEY, amqpMessageProperties.getReceivedRoutingKey(),
							putString)
					.acceptIfNotNull(AmqpHeaders.REDELIVERED, amqpMessageProperties.isRedelivered(), putObject)
					.acceptIfNotNull(AmqpHeaders.REPLY_TO, amqpMessageProperties.getReplyTo(), putObject)
					.acceptIfNotNull(AmqpHeaders.TIMESTAMP, amqpMessageProperties.getTimestamp(), putObject)
					.acceptIfHasText(AmqpHeaders.TYPE, amqpMessageProperties.getType(), putString)
					.acceptIfHasText(AmqpHeaders.RECEIVED_USER_ID, amqpMessageProperties.getReceivedUserId(),
							putString)
					.acceptIfHasText(AmqpHeaders.CONSUMER_TAG, amqpMessageProperties.getConsumerTag(), putString)
					.acceptIfHasText(AmqpHeaders.CONSUMER_QUEUE, amqpMessageProperties.getConsumerQueue(), putString);
			headers.put(AmqpHeaders.LAST_IN_BATCH, amqpMessageProperties.isLastInBatch());

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
			else if (contentType instanceof String string) {
				contentTypeStringValue = string;
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
