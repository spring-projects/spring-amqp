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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import javax.net.ssl.SSLException;

import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionSecurityException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientSessionRemotelyClosedException;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpResourceNotAvailableException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.UncategorizedAmqpException;
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

	private static final String ERROR_UNAUTHORIZED_ACCESS = "amqp:unauthorized-access";

	private static final String ERROR_NOT_FOUND = "amqp:not-found";

	private static final String ERROR_RESOURCE_DELETED = "amqp:resource-deleted";

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
			throw convert(ex);
		}
	}

	static <T> Supplier<T> toUncheckedSupplier(Future<T> future, long timeout) {
		return () -> {
			try {
				if (timeout > 0) {
					return future.get(timeout, TimeUnit.MILLISECONDS);
				}
				else {
					return future.get();
				}
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new UncategorizedAmqpException(ex);
			}
			catch (TimeoutException ex) {
				throw convert(ex);
			}
			catch (ExecutionException ex) {
				throw convert(ex);
			}
		};
	}

	static AmqpException convert(Exception ex) {
		if (ex instanceof AmqpException amqpException) {
			return amqpException;
		}
		else if (ex instanceof ClientException clientException) {
			return convert(clientException);
		}
		else if (ex instanceof TimeoutException) {
			return new AmqpTimeoutException(ex);
		}
		else {
			return new AmqpException(ex);
		}
	}

	static AmqpException convert(ClientException ex) {
		return convert(ex, null);
	}

	static AmqpException convert(ExecutionException ex) {
		Throwable cause = ex.getCause();
		if (cause instanceof ClientException clientException) {
			return convert(clientException);
		}
		else {
			return new AmqpException(cause == null ? ex : cause);
		}
	}

	static AmqpException convert(ClientException ex, @Nullable String format, Object... args) {
		return convert(ex, true, format, args);
	}

	private static AmqpException convert(ClientException ex, boolean checkCause, @Nullable String format,
			Object... args) {

		String message = format != null ? String.format(format, args) : null;
		AmqpException result;
		if (ex.getCause() instanceof SSLException sslException) {
			result = new AmqpAuthenticationException(sslException);
		}
		else if (ex instanceof ClientConnectionSecurityException) {
			result = new AmqpAuthenticationException(ex);
		}
		else {
			String exMessage = ex.getMessage();
			if (isNetworkError(ex)) {
				result = new AmqpConnectException(exMessage != null ? exMessage : "Connection error", ex);
			}
			else if (ex instanceof ClientSessionRemotelyClosedException
					|| ex instanceof ClientLinkRemotelyClosedException) {

				ErrorCondition errorCondition =
						((ClientResourceRemotelyClosedException) ex).getErrorCondition();

				if (isUnauthorizedAccess(errorCondition)) {
					result = new AmqpAuthenticationException(ex);
				}
				else {
					result = new AmqpResourceNotAvailableException(
							exMessage != null ? exMessage : "Resource is not available", ex);
				}
			}
			else if (ex instanceof ClientConnectionRemotelyClosedException clientConnectionRemotelyClosedException) {
				ErrorCondition errorCondition = clientConnectionRemotelyClosedException.getErrorCondition();
				if (isNetworkError(ex) || !isUnauthorizedAccess(errorCondition)) {
					result = new AmqpConnectException(exMessage != null ? exMessage : "Connection error", ex);
				}
				else {
					result = new AmqpException(exMessage != null ? exMessage : "Connection error", ex);
				}
			}
			else {
				result = new AmqpException(message, ex);
			}
		}
		if (checkCause
				&& AmqpException.class.getName().equals(result.getClass().getName())
				&& ex.getCause() instanceof ClientException clientException) {
			// we end up with a generic exception, we try to narrow down with the cause
			result = convert(clientException, false, format, args);
		}
		return result;
	}

	static boolean resourceDeleted(ClientResourceRemotelyClosedException ex) {
		return isResourceDeleted(ex.getErrorCondition());
	}

	static boolean notFound(ClientResourceRemotelyClosedException ex) {
		return isNotFound(ex.getErrorCondition());
	}

	static boolean unauthorizedAccess(ClientResourceRemotelyClosedException ex) {
		return isUnauthorizedAccess(ex.getErrorCondition());
	}

	private static boolean isUnauthorizedAccess(ErrorCondition errorCondition) {
		return errorConditionEquals(errorCondition, ERROR_UNAUTHORIZED_ACCESS);
	}

	private static boolean isNotFound(ErrorCondition errorCondition) {
		return errorConditionEquals(errorCondition, ERROR_NOT_FOUND);
	}

	private static boolean isResourceDeleted(ErrorCondition errorCondition) {
		return errorConditionEquals(errorCondition, ERROR_RESOURCE_DELETED);
	}

	private static boolean errorConditionEquals(ErrorCondition errorCondition, String expected) {
		return expected.equals(errorCondition.condition());
	}

	private static boolean isNetworkError(ClientException e) {
		if (e instanceof ClientConnectionRemotelyClosedException) {
			String message = e.getMessage();
			if (message != null) {
				message = message.toLowerCase();
				return message.contains("connection reset") || message.contains("connection refused");
			}
		}
		return false;
	}

	private ProtonUtils() {
	}

}
