/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client;

import java.util.Date;
import java.util.Map;

import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.StringUtils;

/**
 * Utility methods between ProtonJ and Spring API.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public final class ProtonUtils {

	/**
	 * Convert a Spring AMQP message to a ProtonJ message.
	 * @param message the Spring AMQP message to convert from.
	 * @return the ProtonJ message based on the provided Spring AMQP message.
	 */
	public static Message<?> toProtonMessage(org.springframework.amqp.core.Message message) {
		MessageProperties messageProperties = message.getMessageProperties();
		try {
			String messageId = messageProperties.getMessageId();

			Message<byte[]> protonMessage =
					Message.create(message.getBody())
							.messageId(messageId)
							.contentType(messageProperties.getContentType())
							.priority(messageProperties.getPriority().byteValue())
							.correlationId(messageProperties.getCorrelationId())
							.replyTo(messageProperties.getReplyTo())
							.durable(MessageDeliveryMode.PERSISTENT.equals(messageProperties.getDeliveryMode()));

			// TODO until fix in ProtonJ - returns Message<?> instead of Message<T>
			protonMessage.contentEncoding(messageProperties.getContentEncoding());

			String userId = messageProperties.getUserId();
			if (userId != null) {
				protonMessage.userId(userId.getBytes());
			}

			Date timestamp = messageProperties.getTimestamp();
			if (timestamp == null) {
				timestamp = new Date();
			}
			protonMessage.creationTime(timestamp.getTime());

			String expiration = messageProperties.getExpiration();
			if (StringUtils.hasText(expiration)) {
				protonMessage.timeToLive(Long.parseLong(expiration));
			}

			Map<String, @Nullable Object> headers = messageProperties.getHeaders();
			for (Map.Entry<String, @Nullable Object> entry : headers.entrySet()) {
				protonMessage.property(entry.getKey(), entry.getValue());

			}

			return protonMessage;
		}
		catch (ClientException ex) {
			throw new AmqpClientException(ex);
		}
	}

	private ProtonUtils() {
	}

}
