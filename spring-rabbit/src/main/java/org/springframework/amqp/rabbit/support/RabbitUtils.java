/*
 * Copyright 2002-2010 the original author or authors.
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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
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
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;

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
		if (channel != null) {
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

	public static AmqpException convertRabbitAccessException(Throwable ex) {
		Assert.notNull(ex, "Exception must not be null");
		if (ex instanceof AmqpException) {
			return (AmqpException) ex;
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
				 * sending them back to us).  Does not require a tx.commit.
				 */
				channel.basicRecover(true);
			}
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
				target.setHeader(entry.getKey(), entry.getValue());
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
		// TODO: what about messageCount?
		return target;
	}

	public static BasicProperties extractBasicProperties(Message message, String charset) {
		if (message == null || message.getMessageProperties() == null) {
			return null;
		}
		MessageProperties source = message.getMessageProperties();
		BasicProperties target = new BasicProperties();
		target.setHeaders(source.getHeaders());
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
			} catch (UnsupportedEncodingException ex) {
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

}
