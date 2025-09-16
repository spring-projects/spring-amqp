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

package org.springframework.amqp.rabbit.config;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.MessageBatchRecoverer;
import org.springframework.amqp.rabbit.retry.MessageKeyGenerator;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.NewMessageIdentifier;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.util.backoff.BackOffExecution;

/**
 * {@link MethodInterceptor} implementation that keeps state of message processing using
 * a {@link RetryPolicy}. Method invocations are keyed using a {@link MessageKeyGenerator}.
 *
 * @author Juergen Hoeller
 * @author Stephane Nicoll
 * @author Artem Bilan
 *
 * @since 4.0
 */
public final class StatefulRetryOperationsInterceptor implements MethodInterceptor {

	private static final Log LOGGER = LogFactory.getLog(StatefulRetryOperationsInterceptor.class);

	private final MessageKeyGenerator messageKeyGenerator;

	private final NewMessageIdentifier newMessageIdentifier;

	private final RetryPolicy retryPolicy;

	private final @Nullable MessageRecoverer messageRecoverer;

	private final int stateCacheSize;

	private final Map<Object, RetryState> retryStateCache =
			Collections.synchronizedMap(new LinkedHashMap<>(100, 0.75f, true) {

				@Override
				protected boolean removeEldestEntry(Map.Entry<Object, RetryState> eldest) {
					return size() > StatefulRetryOperationsInterceptor.this.stateCacheSize;
				}

			});

	StatefulRetryOperationsInterceptor(MessageKeyGenerator messageKeyGenerator,
			NewMessageIdentifier newMessageIdentifier, @Nullable RetryPolicy retryPolicy,
			@Nullable MessageRecoverer messageRecoverer, @Nullable Integer stateCacheSize) {

		this.messageKeyGenerator = messageKeyGenerator;
		this.newMessageIdentifier = newMessageIdentifier;
		this.retryPolicy = retryPolicy != null ? retryPolicy : RetryPolicy.builder().delay(Duration.ZERO).build();
		this.stateCacheSize = stateCacheSize != null ? stateCacheSize : 1000;
		this.messageRecoverer = messageRecoverer;
	}

	@Override
	public @Nullable Object invoke(MethodInvocation invocation) throws Throwable {
		@Nullable Object[] args = invocation.getArguments();
		Message message = argToMessage(args);
		Object key = this.messageKeyGenerator.getKey(message);
		if (key == null) { // Means no retry for this message anymore.
			return invocation.proceed();
		}
		RetryState retryState = this.retryStateCache.get(key);
		if (retryState == null || this.newMessageIdentifier.isNew(message)) {
			try {
				return invocation.proceed();
			}
			catch (Throwable ex) {
				this.retryStateCache.put(key, new RetryState(this.retryPolicy.getBackOff().start(), ex));
				throw ex;
			}
		}
		else {
			long time = retryState.backOffExecution().nextBackOff();
			if (time == BackOffExecution.STOP || !this.retryPolicy.shouldRetry(retryState.lastException())) {
				this.retryStateCache.remove(key);
				recover(args[1], retryState.lastException());
				// This is actually a normal outcome. It means the recovery was successful, but we don't want to consume
				// any more messages until the acks and commits are sent for this (problematic) message...
				throw new ImmediateAcknowledgeAmqpException("Recovered message forces ack (if ack mode requires it): "
						+ args[1], retryState.lastException());
			}
			else {
				Thread.sleep(time);
				try {
					Object result = invocation.proceed();
					this.retryStateCache.remove(key);
					return result;
				}
				catch (Throwable ex) {
					this.retryStateCache.put(key, new RetryState(retryState.backOffExecution(), ex));
					throw ex;
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void recover(@Nullable Object arg, Throwable cause) {
		if (this.messageRecoverer == null) {
			LOGGER.warn("Message(s) dropped on recovery: " + arg, cause);
		}
		else if (arg instanceof Message msg) {
			this.messageRecoverer.recover(msg, cause);
		}
		else if (arg instanceof List && this.messageRecoverer instanceof MessageBatchRecoverer recoverer) {
			recoverer.recover((List<Message>) arg, cause);
		}
	}

	private static Message argToMessage(@Nullable Object[] args) {
		Object arg = (args.length > 1) ? args[1] : null;
		if (arg instanceof Message msg) {
			return msg;
		}
		if (arg instanceof List<?> list) {
			return (Message) list.get(0);
		}
		throw new IllegalArgumentException("Expected 2nd arguments to be a message, got " + Arrays.toString(args));
	}

	record RetryState(BackOffExecution backOffExecution, Throwable lastException) {

	}

}
