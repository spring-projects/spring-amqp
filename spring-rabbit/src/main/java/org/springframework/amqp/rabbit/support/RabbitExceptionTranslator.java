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

package org.springframework.amqp.rabbit.support;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.AmqpUnsupportedEncodingException;
import org.springframework.amqp.UncategorizedAmqpException;

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
	}

	public static RuntimeException convertRabbitAccessException(@Nullable Throwable ex) {
		if (ex instanceof AmqpException amqpException) {
			return amqpException;
		}
		if (ex instanceof ShutdownSignalException sigEx) {
			return new AmqpConnectException(sigEx);
		}
		if (ex instanceof ConnectException connEx) {
			return new AmqpConnectException(connEx);
		}
		if (ex instanceof PossibleAuthenticationFailureException) {
			return new AmqpAuthenticationException(ex);
		}
		if (ex instanceof UnsupportedEncodingException) {
			return new AmqpUnsupportedEncodingException(ex);
		}
		if (ex instanceof IOException ioEx) {
			return new AmqpIOException(ioEx);
		}
		if (ex instanceof TimeoutException) {
			return new AmqpTimeoutException(ex);
		}
		if (ex instanceof ConsumerCancelledException) {
			return new org.springframework.amqp.rabbit.support.ConsumerCancelledException(ex);
		}
		if (ex instanceof org.springframework.amqp.rabbit.support.ConsumerCancelledException consumerCancelledException) {
			return consumerCancelledException;
		}
		// fallback
		return new UncategorizedAmqpException(ex);
	}

}
