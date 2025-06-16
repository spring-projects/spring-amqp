/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import java.io.IOException;
import java.util.Collection;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultSaslConfig;
import com.rabbitmq.client.JDKSaslConfig;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.SaslConfig;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.CRDemoMechanism;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Mark Fisher
 * @author Mark Pollack
 * @author Gary Russell
 * @author Artem Bilan
 */
public abstract class RabbitUtils {

	/**
	 * AMQP declare method.
	 */
	public static final int DECLARE_METHOD_ID_10 = 10;

	/**
	 * AMQP consume method.
	 */
	public static final int CONSUME_METHOD_ID_20 = 20;

	/**
	 * AMQP exchange class id.
	 */
	public static final int EXCHANGE_CLASS_ID_40 = 40;

	/**
	 * AMQP queue class id.
	 */
	public static final int QUEUE_CLASS_ID_50 = 50;

	/**
	 * AMQP basic class id.
	 */
	public static final int BASIC_CLASS_ID_60 = 60;

	/**
	 * AMQP Connection protocol class id.
	 */
	public static final int CONNECTION_PROTOCOL_CLASS_ID_10 = 10;

	/**
	 * AMQP Channel protocol class id.
	 */
	public static final int CHANNEL_PROTOCOL_CLASS_ID_20 = 20;

	private static final Log LOGGER = LogFactory.getLog(RabbitUtils.class);

	private static final ThreadLocal<Boolean> physicalCloseRequired = new ThreadLocal<>(); // NOSONAR - lower case

	/**
	 * Close the given RabbitMQ Connection and ignore any thrown exception. This is useful for typical
	 * <code>finally</code> blocks in manual RabbitMQ code.
	 * @param connection the RabbitMQ Connection to close (may be <code>null</code>)
	 */
	public static void closeConnection(@Nullable Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			}
			catch (AlreadyClosedException ace) {
				// empty
			}
			catch (Exception ex) {
				LOGGER.debug("Ignoring Connection exception - assuming already closed: " + ex.getMessage(), ex);
			}
		}
	}

	/**
	 * Close the given RabbitMQ Channel and ignore any thrown exception. This is useful for typical <code>finally</code>
	 * blocks in manual RabbitMQ code.
	 * @param channel the RabbitMQ Channel to close (may be <code>null</code>)
	 */
	public static void closeChannel(@Nullable Channel channel) {
		if (channel != null) {
			try {
				channel.close();
			}
			catch (AlreadyClosedException ace) {
				// empty
			}
			catch (IOException ex) {
				LOGGER.debug("Could not close RabbitMQ Channel", ex);
			}
			catch (ShutdownSignalException sig) {
				if (!isNormalShutdown(sig)) {
					LOGGER.debug("Unexpected exception on closing RabbitMQ Channel", sig);
				}
			}
			catch (Exception ex) {
				LOGGER.debug("Unexpected exception on closing RabbitMQ Channel", ex);
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
		}
		catch (IOException ex) {
			throw new AmqpIOException(ex);
		}
	}

	public static void rollbackIfNecessary(Channel channel) {
		Assert.notNull(channel, "Channel must not be null");
		try {
			channel.txRollback();
		}
		catch (IOException ex) {
			throw new AmqpIOException(ex);
		}
	}

	public static void closeMessageConsumer(Channel channel, Collection<String> consumerTags, boolean transactional) {
		if (!channel.isOpen() && !(channel instanceof ChannelProxy proxy
					&& proxy.getTargetChannel() instanceof AutorecoveringChannel)
				&& !(channel instanceof AutorecoveringChannel)) {
			return;
		}
		try {
			for (String consumerTag : consumerTags) {
				cancel(channel, consumerTag);
			}
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
		}
		catch (Exception ex) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(ex);
		}
	}

	public static void cancel(Channel channel, String consumerTag) {
		try {
			channel.basicCancel(consumerTag);
		}
		catch (AlreadyClosedException e) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace(channel + " is already closed", e);
			}
		}
		catch (Exception e) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Error performing 'basicCancel' on " + channel, e);
			}
		}
	}

	/**
	 * Declare to that broker that a channel is going to be used transactionally, and convert exceptions that arise.
	 * @param channel the channel to use
	 */
	public static void declareTransactional(Channel channel) {
		try {
			channel.txSelect();
		}
		catch (IOException e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	/**
	 * Sets a ThreadLocal indicating the channel MUST be physically closed.
	 * @param channel the channel.
	 * @param b true if the channel must be closed (if it's a proxy).
	 */
	public static void setPhysicalCloseRequired(Channel channel, boolean b) {
		if (channel instanceof ChannelProxy) {
			physicalCloseRequired.set(b);
		}
	}

	/**
	 * Gets and removes a ThreadLocal indicating the channel MUST be physically closed.
	 * @return true if the channel must be physically closed
	 */
	public static boolean isPhysicalCloseRequired() {
		Boolean mustClose = physicalCloseRequired.get();
		return mustClose != null && mustClose;
	}

	/**
	 * Clear the physicalCloseRequired flag.
	 */
	public static void clearPhysicalCloseRequired() {
		physicalCloseRequired.remove();
	}

	/**
	 * Return true if the {@link ShutdownSignalException} reason is AMQP.Connection.Close and
	 * the reply code was AMQP.REPLY_SUCCESS (200) and the text equals "OK".
	 * @param sig the exception.
	 * @return true for a normal connection close.
	 */
	public static boolean isNormalShutdown(ShutdownSignalException sig) {
		Method shutdownReason = sig.getReason();
		return shutdownReason instanceof AMQP.Connection.Close closeReason
				&& AMQP.REPLY_SUCCESS == closeReason.getReplyCode()
				&& "OK".equals(closeReason.getReplyText());
	}

	/**
	 * Return true if the {@link ShutdownSignalException} reason is AMQP.Channel.Close and
	 * the reply code was AMQP.REPLY_SUCCESS (200) and the text equals "OK".
	 * @param sig the exception.
	 * @return true for a normal channel close.
	 */
	public static boolean isNormalChannelClose(ShutdownSignalException sig) {
		Method shutdownReason = sig.getReason();
		return isNormalShutdown(sig) ||
				(shutdownReason instanceof AMQP.Channel.Close closeReason
					&& AMQP.REPLY_SUCCESS == closeReason.getReplyCode()
					&& "OK".equals(closeReason.getReplyText()));
	}

	/**
	 * Return true if the {@link ShutdownSignalException} reason is AMQP.Channel.Close
	 * and the operation that failed was exchangeDeclare or queueDeclare.
	 * @param sig the exception.
	 * @return true if the failure meets the conditions.
	 */
	public static boolean isPassiveDeclarationChannelClose(ShutdownSignalException sig) {
		Method shutdownReason = sig.getReason();
		return shutdownReason instanceof AMQP.Channel.Close closeReason // NOSONAR boolean complexity
				&& AMQP.NOT_FOUND == closeReason.getReplyCode()
				&& ((closeReason.getClassId() == EXCHANGE_CLASS_ID_40
					|| closeReason.getClassId() == QUEUE_CLASS_ID_50)
					&& closeReason.getMethodId() == DECLARE_METHOD_ID_10);
	}

	/**
	 * Return true if the {@link ShutdownSignalException} reason is AMQP.Channel.Close
	 * and the operation that failed was basicConsumer and the failure text contains
	 * "exclusive".
	 * @param sig the exception.
	 * @return true if the declaration failed because of an exclusive queue.
	 */
	public static boolean isExclusiveUseChannelClose(ShutdownSignalException sig) {
		Method shutdownReason = sig.getReason();
		return shutdownReason instanceof AMQP.Channel.Close closeReason // NOSONAR boolean complexity
				&& AMQP.ACCESS_REFUSED == closeReason.getReplyCode()
				&& closeReason.getClassId() == BASIC_CLASS_ID_60
				&& closeReason.getMethodId() == CONSUME_METHOD_ID_20
				&& closeReason.getReplyText().contains("exclusive");
	}

	/**
	 * Return true if there is a {@link ShutdownSignalException} in the cause tree and its
	 * reason is "PRECONDITION_FAILED" and the operation being performed was queueDeclare.
	 * This can happen if a queue has mismatched properties (auto-delete etc) or arguments
	 * (x-message-ttl etc).
	 * @param e the exception.
	 * @return true if the exception was due to queue declaration precondition failed.
	 * @since 1.6
	 */
	public static boolean isMismatchedQueueArgs(Exception e) {
		Throwable cause = e;
		ShutdownSignalException sig = null;
		while (cause != null && sig == null) {
			if (cause instanceof ShutdownSignalException shutdownSignalException) {
				sig = shutdownSignalException;
			}
			cause = cause.getCause();
		}
		if (sig == null) {
			return false;
		}

		Method shutdownReason = sig.getReason();
		return shutdownReason instanceof AMQP.Channel.Close closeReason
				&& AMQP.PRECONDITION_FAILED == closeReason.getReplyCode()
				&& closeReason.getClassId() == QUEUE_CLASS_ID_50
				&& closeReason.getMethodId() == DECLARE_METHOD_ID_10;
	}

	/**
	 * Return true if there is a {@link ShutdownSignalException} in the cause tree and its
	 * reason is "COMMAND_INVALID" and the operation being performed was exchangeDeclare.
	 * For example attempting to declare an exchange that is not supported by the broker or
	 * its plugins.
	 * @param e the exception.
	 * @return true if the exception was due to exchange declaration failed.
	 * @since 1.6
	 */
	public static boolean isExchangeDeclarationFailure(Exception e) {
		Throwable cause = e;
		ShutdownSignalException sig = null;
		while (cause != null && sig == null) {
			if (cause instanceof ShutdownSignalException shutdownSignalException) {
				sig = shutdownSignalException;
			}
			cause = cause.getCause();
		}
		if (sig == null) {
			return false;
		}

		Method shutdownReason = sig.getReason();
		return shutdownReason instanceof AMQP.Channel.Close closeReason
				&& AMQP.PRECONDITION_FAILED == closeReason.getReplyCode()
				&& closeReason.getClassId() == EXCHANGE_CLASS_ID_40
				&& closeReason.getMethodId() == DECLARE_METHOD_ID_10;
	}

	/**
	 * Return the negotiated frame_max.
	 * @param connectionFactory the connection factory.
	 * @return the size or -1 if it cannot be determined.
	 */
	public static int getMaxFrame(ConnectionFactory connectionFactory) {
		try (Connection	connection = connectionFactory.createConnection()) {
			com.rabbitmq.client.Connection rcon = connection.getDelegate();
			if (rcon != null) {
				return rcon.getFrameMax();
			}
		}
		catch (@SuppressWarnings("unused") RuntimeException e) {
			// NOSONAR
		}
		return -1;
	}

	/**
	 * Convert a String value to a {@link SaslConfig}.
	 * Valid string values:
	 * <ul>
	 * <li>{@code DefaultSaslConfig.PLAIN}</li>
	 * <li>{@code DefaultSaslConfig.EXTERNAL}</li>
	 * <li>{@code JDKSaslConfig}</li>
	 * <li>{@code CRDemoSaslConfig}</li>
	 * </ul>
	 * @param saslConfig the string value.
	 * @param connectionFactory the connection factory to get the name, pw, host.
	 * @return the saslConfig.
	 */
	public static SaslConfig stringToSaslConfig(String saslConfig,
			com.rabbitmq.client.ConnectionFactory connectionFactory) {

		return switch (saslConfig) {
			case "DefaultSaslConfig.PLAIN" -> DefaultSaslConfig.PLAIN;
			case "DefaultSaslConfig.EXTERNAL" -> DefaultSaslConfig.EXTERNAL;
			case "JDKSaslConfig" -> new JDKSaslConfig(connectionFactory);
			case "CRDemoSaslConfig" -> new CRDemoMechanism.CRDemoSaslConfig();
			default -> throw new IllegalStateException("Unrecognized SaslConfig: " + saslConfig);
		};
	}

	/**
	 * Determine whether the exception is due to an access refused for an exclusive consumer.
	 * @param exception the exception.
	 * @return true if access refused.
	 * @since 3.1
	 */
	public static boolean exclusiveAccesssRefused(Exception exception) {
		return exception.getCause() instanceof IOException
				&& exception.getCause().getCause() instanceof ShutdownSignalException sse1
				&& isExclusiveUseChannelClose(sse1)
				|| exception.getCause() instanceof ShutdownSignalException sse2
						&& isExclusiveUseChannelClose(sse2);
	}

}
