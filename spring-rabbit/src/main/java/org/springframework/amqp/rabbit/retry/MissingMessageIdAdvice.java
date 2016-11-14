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

package org.springframework.amqp.rabbit.retry;

import java.util.UUID;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.retry.policy.RetryContextCache;
import org.springframework.util.Assert;

/**
 * Advice that can be placed in the listener delegate's advice chain to
 * enhance the message with an ID if not present.
 * If an exception is caught on a redelivered message, rethrows it as an {@link AmqpRejectAndDontRequeueException}
 * which signals the container to NOT requeue the message (otherwise we'd have infinite
 * immediate retries).
 * If so configured, the broker can send the message to a DLE/DLQ.
 * Must be placed before the retry interceptor in the advice chain.
 * @author Gary Russell
 * @since 1.1.2
 *
 */
public class MissingMessageIdAdvice implements MethodInterceptor {

	private static final Log logger = LogFactory.getLog(MissingMessageIdAdvice.class);

	private final RetryContextCache retryContextCache;

	public MissingMessageIdAdvice(RetryContextCache retryContextCache) {
		Assert.notNull(retryContextCache, "RetryContextCache must not be null");
		this.retryContextCache = retryContextCache;
	}

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		String id = null;
		Message message = null;
		boolean redelivered = false;
		try {
			message = (Message) invocation.getArguments()[1];
			MessageProperties messageProperties = message.getMessageProperties();
			if (messageProperties.getMessageId() == null) {
				id = UUID.randomUUID().toString();
				messageProperties.setMessageId(id);
			}
			redelivered = messageProperties.isRedelivered();
			return invocation.proceed();
		}
		catch (Exception e) {
			if (id != null && redelivered) {
				if (logger.isDebugEnabled()) {
					logger.debug("Canceling delivery of retried message that has no ID");
				}
				throw new ListenerExecutionFailedException("Cannot retry message without an ID",
						new AmqpRejectAndDontRequeueException(e), message);
			}
			else {
				throw e;
			}
		}
		finally {
			if (id != null) {
				this.retryContextCache.remove(id);
			}
		}
	}

}
