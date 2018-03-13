/*
 * Copyright 2002-2018 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.listener.MessageRejectedWhileStoppingException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.util.Assert;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;

/**
 * @author Mark Fisher
 * @author Mark Pollack
 * @author Gary Russell
 * @author Artem Bilan
 */
public abstract class RabbitUtils {

	private static final Log logger = LogFactory.getLog(RabbitUtils.class);

	private static final ThreadLocal<Boolean> physicalCloseRequired = new ThreadLocal<>();

	/**
	 * Close the given RabbitMQ Connection and ignore any thrown exception. This is useful for typical
	 * <code>finally</code> blocks in manual RabbitMQ code.
	 * @param connection the RabbitMQ Connection to close (may be <code>null</code>)
	 */
	public static void closeConnection(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			}
			catch (AlreadyClosedException ace) {
				// empty
			}
			catch (Exception ex) {
				logger.debug("Ignoring Connection exception - assuming already closed: " + ex.getMessage(), ex);
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
			}
			catch (AlreadyClosedException ace) {
				// empty
			}
			catch (IOException ex) {
				logger.debug("Could not close RabbitMQ Channel", ex);
			}
			catch (ShutdownSignalException sig) {
				if (!isNormalShutdown(sig)) {
					logger.debug("Unexpected exception on closing RabbitMQ Channel", sig);
				}
			}
			catch (Exception ex) {
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
		if (!channel.isOpen() && !(channel instanceof ChannelProxy
					&& ((ChannelProxy) channel).getTargetChannel() instanceof AutorecoveringChannel)
				&& !(channel instanceof AutorecoveringChannel)) {
			return;
		}
		try {
			for (String consumerTag : consumerTags) {
				try {
					channel.basicCancel(consumerTag);
				}
				catch (IOException e) {
					if (logger.isDebugEnabled()) {
						logger.debug("Error performing 'basicCancel'", e);
					}
				}
				catch (AlreadyClosedException e) {
					if (logger.isTraceEnabled()) {
						logger.trace(channel + " is already closed");
					}
				}
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
		if (mustClose == null) {
			mustClose = Boolean.FALSE;
		}
		else {
			physicalCloseRequired.remove();
		}
		return mustClose;
	}

	/**
	 * Return true if the {@link ShutdownSignalException} reason is AMQP.Connection.Close and
	 * the reply code was AMQP.REPLY_SUCCESS (200) and the text equals "OK".
	 * @param sig the exception.
	 * @return true for a normal connection close.
	 */
	public static boolean isNormalShutdown(ShutdownSignalException sig) {
		Method shutdownReason = sig.getReason();
		return shutdownReason instanceof AMQP.Connection.Close
				&& AMQP.REPLY_SUCCESS == ((AMQP.Connection.Close) shutdownReason).getReplyCode()
				&& "OK".equals(((AMQP.Connection.Close) shutdownReason).getReplyText());
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
				(shutdownReason instanceof AMQP.Channel.Close
					&& AMQP.REPLY_SUCCESS == ((AMQP.Channel.Close) shutdownReason).getReplyCode()
					&& "OK".equals(((AMQP.Channel.Close) shutdownReason).getReplyText()));
	}

	/**
	 * Return true if the {@link ShutdownSignalException} reason is AMQP.Channel.Close
	 * and the operation that failed was exchangeDeclare or queueDeclare.
	 * @param sig the exception.
	 * @return true if the failure meets the conditions.
	 */
	public static boolean isPassiveDeclarationChannelClose(ShutdownSignalException sig) {
		Method shutdownReason = sig.getReason();
		return shutdownReason instanceof AMQP.Channel.Close
				&& AMQP.NOT_FOUND == ((AMQP.Channel.Close) shutdownReason).getReplyCode()
				&& ((((AMQP.Channel.Close) shutdownReason).getClassId() == 40 // exchange
					|| ((AMQP.Channel.Close) shutdownReason).getClassId() == 50) // queue
					&& ((AMQP.Channel.Close) shutdownReason).getMethodId() == 10); // declare
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
		return shutdownReason instanceof AMQP.Channel.Close
				&& AMQP.ACCESS_REFUSED == ((AMQP.Channel.Close) shutdownReason).getReplyCode()
				&& ((AMQP.Channel.Close) shutdownReason).getClassId() == 60 // basic
				&& ((AMQP.Channel.Close) shutdownReason).getMethodId() == 20 // consume
				&& ((AMQP.Channel.Close) shutdownReason).getReplyText().contains("exclusive");
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
			if (cause instanceof ShutdownSignalException) {
				sig = (ShutdownSignalException) cause;
			}
			cause = cause.getCause();
		}
		if (sig == null) {
			return false;
		}
		else {
			Method shutdownReason = sig.getReason();
			return shutdownReason instanceof AMQP.Channel.Close
					&& AMQP.PRECONDITION_FAILED == ((AMQP.Channel.Close) shutdownReason).getReplyCode()
					&& ((AMQP.Channel.Close) shutdownReason).getClassId() == 50 // queue
					&& ((AMQP.Channel.Close) shutdownReason).getMethodId() == 10; // declare
		}
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
			if (cause instanceof ShutdownSignalException) {
				sig = (ShutdownSignalException) cause;
			}
			cause = cause.getCause();
		}
		if (sig == null) {
			return false;
		}
		else {
			Method shutdownReason = sig.getReason();
			return shutdownReason instanceof AMQP.Connection.Close
					&& AMQP.COMMAND_INVALID == ((AMQP.Connection.Close) shutdownReason).getReplyCode()
					&& ((AMQP.Connection.Close) shutdownReason).getClassId() == 40 // exchange
					&& ((AMQP.Connection.Close) shutdownReason).getMethodId() == 10; // declare
		}
	}

	/**
	 * Determine whether a message should be requeued; returns true if the throwable is a
	 * {@link MessageRejectedWhileStoppingException} or defaultRequeueRejected is true and
	 * there is not an {@link AmqpRejectAndDontRequeueException} in the cause chain.
	 * @param defaultRequeueRejected the default requeue rejected.
	 * @param throwable the throwable.
	 * @param logger the logger to use for debug.
	 * @return true to requeue.
	 * @since 2.0
	 * @deprecated in favor of {@code ContainerUtils#shouldRequeue(boolean, Throwable, Log)}.
	 */
	@Deprecated
	public static boolean shouldRequeue(boolean defaultRequeueRejected, Throwable throwable, Log logger) {
		logger.warn("Use ContainerUtils.shouldRequeue()");
		// compare class by name to avoid tangle
		boolean shouldRequeue = defaultRequeueRejected ||
				throwable.getClass().getName().equals(
						"org.springframework.amqp.rabbit.listener.MessageRejectedWhileStoppingException");
		Throwable t = throwable;
		while (shouldRequeue && t != null) {
			if (t instanceof AmqpRejectAndDontRequeueException) {
				shouldRequeue = false;
			}
			t = t.getCause();
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Rejecting messages (requeue=" + shouldRequeue + ")");
		}
		return shouldRequeue;
	}

}
