/*
 * Copyright 2002-2014 the original author or authors.
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
import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Mark Fisher
 * @author Mark Pollack
 * @author Gary Russell
 */
public abstract class RabbitUtils {

	public static final int DEFAULT_PORT = AMQP.PROTOCOL.PORT;

	private static final Log logger = LogFactory.getLog(RabbitUtils.class);

	private static final ThreadLocal<Boolean> physicalCloseRequired = new ThreadLocal<Boolean>();

	private static final Method shutDownSignalReasonMethod;

	static {
		Method method = null;
		try {
			method = ReflectionUtils.findMethod(ShutdownSignalException.class, "getReason");
		}
		finally {
			shutDownSignalReasonMethod = method;
		}
	}

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
			catch (IOException ex) {
				logger.debug("Could not close RabbitMQ Channel", ex);
			}
			catch (ShutdownSignalException sig) {
				if (!isNormalShutdown(sig)) {
					logger.debug("Unexpected exception on closing RabbitMQ Channel", sig);
				}
			}
			catch (Throwable ex) {
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

	public static void closeMessageConsumer(Channel channel, Collection<String> consumerTags, boolean transactional) {
		if (!channel.isOpen()) {
			return;
		}
		try {
			synchronized(consumerTags) {
				for (String consumerTag : consumerTags) {
					channel.basicCancel(consumerTag);
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
		} catch (Exception ex) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(ex);
		}
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
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	/**
	 * Sets a ThreadLocal indicating the channel MUST be physically closed.
	 * @param b true if the channel must be closed.
	 */
	public static void setPhysicalCloseRequired(boolean b) {
		physicalCloseRequired.set(b);
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

	public static boolean isNormalShutdown(ShutdownSignalException sig) {
		Object shutdownReason = determineShutdownReason(sig);
		return shutdownReason instanceof AMQP.Connection.Close
				&& AMQP.REPLY_SUCCESS == ((AMQP.Connection.Close) shutdownReason).getReplyCode()
				&& "OK".equals(((AMQP.Connection.Close) shutdownReason).getReplyText());
	}

	public static boolean isNormalChannelClose(ShutdownSignalException sig) {
		Object shutdownReason = determineShutdownReason(sig);
		return shutdownReason instanceof AMQP.Channel.Close
				&& AMQP.REPLY_SUCCESS == ((AMQP.Channel.Close) shutdownReason).getReplyCode()
				&& "OK".equals(((AMQP.Channel.Close) shutdownReason).getReplyText());
	}

	protected static Object determineShutdownReason(ShutdownSignalException sig) {
		if (shutDownSignalReasonMethod == null) {
			return false;
		}
		try {
			return ReflectionUtils.invokeMethod(shutDownSignalReasonMethod, sig);
		}
		catch (Exception e) {
			return false;
		}
	}

}
