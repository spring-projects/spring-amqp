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

package org.springframework.amqp.rabbit.connection;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.ConnectException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpUnsupportedEncodingException;
import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.LongString;

/**
 * @author Mark Fisher
 * @author Mark Pollack
 */
public abstract class RabbitUtils {

	public static final int DEFAULT_PORT = AMQP.PROTOCOL.PORT;

	private static final Log logger = LogFactory.getLog(RabbitUtils.class);

	/**
	 * Close the given RabbitMQ Connection and ignore any thrown exception. This is useful for typical
	 * <code>finally</code> blocks in manual RabbitMQ code.
	 * @param connection the RabbitMQ Connection to close (may be <code>null</code>)
	 */
	public static void closeConnection(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (Exception ex) {
				logger.debug("Ignoring Connection exception - assuming already closed: " + ex);
			}
		}
	}

	/**
	 * Close the given RabbitMQ Channel and ignore any thrown exception. This is useful for typical <code>finally</code>
	 * blocks in manual RabbitMQ code.
	 * @param channel the RabbitMQ Channel to close (may be <code>null</code>)
	 */
	public static void closeChannel(Channel channel) {
		if (channel != null && channel.isOpen()) {
			try {
				channel.close();
			} catch (IOException ex) {
				logger.debug("Could not close RabbitMQ Channel", ex);
			} catch (Throwable ex) {
				logger.debug("Unexpected exception on closing RabbitMQ Channel", ex);
			}
		}
	}

	/**
	 * Commit the Channel if not within a JTA transaction.
	 * @param channel the RabbitMQ Channel to commit
	 */
	public static void commitIfNecessary(Channel channel) {
		Assert.notNull(channel, "Channel must not be null");
		try {
			channel.txCommit();
		} catch (IOException ex) {
			throw new AmqpIOException(ex);
		}
	}

	public static void rollbackIfNecessary(Channel channel) {
		Assert.notNull(channel, "Channel must not be null");
		try {
			channel.txRollback();
		} catch (IOException ex) {
			throw new AmqpIOException(ex);
		}
	}

	public static RuntimeException convertRabbitAccessException(Throwable ex) {
		Assert.notNull(ex, "Exception must not be null");
		if (ex instanceof AmqpException) {
			return (AmqpException) ex;
		}
		if (ex instanceof ShutdownSignalException) {
			return new AmqpConnectException((ShutdownSignalException) ex);
		}
		if (ex instanceof ConnectException) {
			return new AmqpConnectException((ConnectException) ex);
		}
		if (ex instanceof IOException) {
			return new AmqpIOException((IOException) ex);
		}
		if (ex instanceof UnsupportedEncodingException) {
			return new AmqpUnsupportedEncodingException(ex);
		}
		// fallback
		return new UncategorizedAmqpException(ex);
	}

	public static void closeMessageConsumer(Channel channel, String consumerTag, boolean transactional) {
		if (!channel.isOpen()) {
			return;
		}
		try {
			channel.basicCancel(consumerTag);
			if (transactional) {
				/*
				 * Re-queue in-flight messages if any (after the consumer is cancelled to prevent the broker from simply
				 * sending them back to us). Does not require a tx.commit.
				 */
				channel.basicRecover(true);
			}
			/*
			 * If not transactional then we are auto-acking (at least as of 1.0.0.M2) so there is nothing to recover.
			 * Messages are going to be lost in general.
			 */
		} catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
	}

	public static MessageProperties createMessageProperties(final BasicProperties source, final Envelope envelope,
			final String charset) {
		MessageProperties target = new MessageProperties();
		Map<String, Object> headers = source.getHeaders();
		if (!CollectionUtils.isEmpty(headers)) {
			for (Map.Entry<String, Object> entry : headers.entrySet()) {
				Object value = entry.getValue();
				if (value instanceof LongString) {
					try {
						LongString longString = (LongString) value;
						if (longString.length() <= 1024) {
							value = new String(longString.getBytes(), charset);
						}
						else {
							value = longString.getStream();
						}
					}
					catch (Exception e) {
						throw convertRabbitAccessException(e);
					}
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
			target.setReplyTo(new Address(replyTo));
		}
		if (envelope != null) {
			target.setReceivedExchange(envelope.getExchange());
			target.setReceivedRoutingKey(envelope.getRoutingKey());
			target.setRedelivered(envelope.isRedeliver());
			target.setDeliveryTag(envelope.getDeliveryTag());
		}
		return target;
	}

	public static BasicProperties extractBasicProperties(Message message, String charset) {
		if (message == null || message.getMessageProperties() == null) {
			return null;
		}
		MessageProperties source = message.getMessageProperties();
		BasicProperties target = new BasicProperties();
		target.setHeaders(convertHeadersIfNecessary(source.getHeaders()));
		target.setTimestamp(source.getTimestamp());
		target.setMessageId(source.getMessageId());
		target.setUserId(source.getUserId());
		target.setAppId(source.getAppId());
		target.setClusterId(source.getClusterId());
		target.setType(source.getType());
		MessageDeliveryMode deliveryMode = source.getDeliveryMode();
		if (deliveryMode != null) {
			target.setDeliveryMode(MessageDeliveryMode.toInt(deliveryMode));
		}
		target.setExpiration(source.getExpiration());
		target.setPriority(source.getPriority());
		target.setContentType(source.getContentType());
		target.setContentEncoding(source.getContentEncoding());
		byte[] correlationId = source.getCorrelationId();
		if (correlationId != null && correlationId.length > 0) {
			try {
				target.setCorrelationId(new String(correlationId, charset));
			}
			catch (UnsupportedEncodingException ex) {
				throw new AmqpUnsupportedEncodingException(ex);
			}
		}
		Address replyTo = source.getReplyTo();
		if (replyTo != null) {
			target.setReplyTo(replyTo.toString());
		}
		return target;
	}

	/**
	 * Declare to that broker that a channel is going to be used transactionally, and convert exceptions that arise.
	 * 
	 * @param channel the channel to use
	 */
	public static void declareTransactional(Channel channel) {
		try {
			channel.txSelect();
		} catch (IOException e) {
			throw RabbitUtils.convertRabbitAccessException(e);
		}
	}

	private static Map<String, Object> convertHeadersIfNecessary(Map<String, Object> headers) {
		if (CollectionUtils.isEmpty(headers)) {
			return Collections.<String, Object>emptyMap();
		}
		Map<String, Object> writableHeaders = new HashMap<String, Object>();
		for (Map.Entry<String, Object> entry : headers.entrySet()) {
			writableHeaders.put(entry.getKey(), convertHeaderValueIfNecessary(entry.getValue()));
		}
		return writableHeaders;
	}

	private static Object convertHeaderValueIfNecessary(Object value) {
		boolean valid = (value instanceof String)
				|| (value instanceof byte[])
				|| (value instanceof Boolean)
				|| (value instanceof LongString)
				|| (value instanceof Integer)
				|| (value instanceof Long)
				|| (value instanceof Float)
				|| (value instanceof Double)
				|| (value instanceof BigDecimal)
				|| (value instanceof Short)
				|| (value instanceof Byte)
				|| (value instanceof Date)
				|| (value instanceof List)
				|| (value instanceof Map);
		if (!valid && value != null) {
			value = value.toString();
		}
		return value;
	}

}
