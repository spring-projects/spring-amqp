/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpUnsupportedEncodingException;
import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitMessageProperties;
import org.springframework.beans.BeanUtils;
import org.springframework.util.Assert;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * @author Mark Fisher
 * @author Mark Pollack
 */
public abstract class RabbitUtils {

	private static final Log logger = LogFactory.getLog(RabbitUtils.class);


	public static final int DEFAULT_PORT = AMQP.PROTOCOL.PORT;


	/**
	 * Close the given RabbitMQ Connection and ignore any thrown exception.
	 * This is useful for typical <code>finally</code> blocks in manual RabbitMQ code.
	 * @param connection the RabbitMQ Connection to close (may be <code>null</code>)
	 */
	public static void closeConnection(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			}
			catch (Exception ex) {
				logger.debug("Ignoring Connection exception - assuming already closed: " + ex);
			}
		}
	}

	/**
	 * Close the given RabbitMQ Channel and ignore any thrown exception.
	 * This is useful for typical <code>finally</code> blocks in manual RabbitMQ code.
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
			catch (Throwable ex) {
				logger.debug("Unexpected exception on closing RabbitMQ Channel", ex);
			}
		}
	}

	/**
	 * Commit the Channel if not within a JTA transaction.
	 * @param channel the RabbitMQ Channel to commit
	 * @throws IOException 
	 * @throws IOException if committing failed
	 */
	public static void commitIfNecessary(Channel channel) {
		Assert.notNull(channel, "Channel must not be null");
		try {
			channel.txCommit();
		}
		catch (IOException ex) {
			logger.error("Could not commit RabbitMQ Channel", ex);
		}
	}

	public static void rollbackIfNecessary(Channel channel) {
		Assert.notNull(channel, "Channel must not be null");
		try {
			channel.txRollback();
		}
		catch (IOException ex) {
			logger.error("Could not rollback RabbitMQ Channel", ex);
		}
	}

	public static AmqpException convertRabbitAccessException(Exception ex) {
		Assert.notNull(ex, "Exception must not be null");
		if (ex instanceof IOException) {
			return new AmqpIOException((IOException) ex);
		}
		if (ex instanceof UnsupportedEncodingException) {
			return new AmqpUnsupportedEncodingException((UnsupportedEncodingException) ex);
		}
		//fallback
		return new UncategorizedAmqpException(ex);
	}

	public static void closeMessageConsumer(Channel channel, String consumerTag) {
		try {
			channel.basicCancel(consumerTag);		
		}
		catch (Exception ex) {
			convertRabbitAccessException(ex);
		}
	}

	public static com.rabbitmq.client.AMQP.BasicProperties extractBasicProperties(Message message, String encoding) {
		MessageProperties sourceProperties = message.getMessageProperties();
		if (sourceProperties instanceof RabbitMessageProperties) {
			return ((RabbitMessageProperties) sourceProperties).getBasicProperties();
		}
		RabbitMessageProperties targetProperties = new RabbitMessageProperties();
		PropertyDescriptor[] descriptors = BeanUtils.getPropertyDescriptors(MessageProperties.class);
		for (PropertyDescriptor descriptor : descriptors) {
			Method getter = descriptor.getReadMethod();
			Method setter = descriptor.getWriteMethod();
			if (getter != null || setter != null) {
				try {
					Object value = getter.invoke(sourceProperties, new Object[0]);
					if (value != null) {
						setter.invoke(targetProperties, value);
					}
				}
				catch (Exception e) {
					// ignore
				}
			}
		}
		for (Map.Entry<String, Object> entry : sourceProperties.getHeaders().entrySet()) {
			targetProperties.setHeader(entry.getKey(), entry.getValue());
		}
		return targetProperties.getBasicProperties();		
	}

}
