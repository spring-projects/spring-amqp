/*
 * Copyright 2002-2016 the original author or authors.
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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.util.concurrent.TimeoutException;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.AmqpUnsupportedEncodingException;
import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.util.Assert;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Translates Rabbit Exceptions to the {@link AmqpException} class
 * hierarchy.
 * This functionality was previously in RabbitUtils, but that
 * caused a package tangle.
 *
 * @author Gary Russell
 * @since 1.2
 *
 */
public final class RabbitExceptionTranslator {

	private RabbitExceptionTranslator() {
		super();
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
		if (ex instanceof PossibleAuthenticationFailureException) {
			return new AmqpAuthenticationException(ex);
		}
		if (ex instanceof UnsupportedEncodingException) {
			return new AmqpUnsupportedEncodingException(ex);
		}
		if (ex instanceof IOException) {
			return new AmqpIOException((IOException) ex);
		}
		if (ex instanceof TimeoutException) {
			return new AmqpTimeoutException(ex);
		}
		if (ex instanceof ConsumerCancelledException) {
			return new org.springframework.amqp.rabbit.support.ConsumerCancelledException(ex);
		}
		if (ex instanceof org.springframework.amqp.rabbit.support.ConsumerCancelledException) {
			throw (org.springframework.amqp.rabbit.support.ConsumerCancelledException) ex;
		}
		// fallback
		return new UncategorizedAmqpException(ex);
	}

}
