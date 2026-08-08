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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.MessageBatchRecoverer;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.core.retry.RetryListener;
import org.springframework.core.retry.RetryOperations;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.Retryable;

/**
 * Convenient factory bean for creating a stateless retry interceptor for use in a message listener container, giving
 * you a large amount of control over the behaviour of a container when a listener fails. To control the number of retry
 * attempts or the backoff in between attempts, supply a customized {@link RetryPolicy}. Stateless retry is appropriate
 * if your listener can be called repeatedly between failures with no side effects. The semantics of stateless retry
 * mean that a listener exception is not propagated to the container until the retry attempts are exhausted. When the
 * retry attempts are exhausted it can be processed using a {@link MessageRecoverer} if one is provided, in the same
 * transaction (in which case no exception is propagated). If a recoverer is not provided, the exception will be
 * propagated and the message may be redelivered if the channel is transactional.
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Stephane Nicoll
 * @author Jun Cho
 *
 * @see RetryOperations#execute(Retryable)
 */
public class StatelessRetryOperationsInterceptorFactoryBean extends AbstractRetryOperationsInterceptorFactoryBean {

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	private @Nullable RetryListener retryListener;

	/**
	 * Set a {@link RetryListener} for the target stateless interceptor. If multiple
	 * listeners are needed, use a
	 * {@link org.springframework.core.retry.support.CompositeRetryListener}.
	 * @param retryListener the retry listener to use
	 * @since 4.1.1
	 */
	public void setRetryListener(RetryListener retryListener) {
		this.retryListener = retryListener;
	}

	@Override
	public StatelessRetryOperationsInterceptor getObject() {
		StatelessRetryOperationsInterceptor interceptor =
				new StatelessRetryOperationsInterceptor(getRetryPolicy(), this::recover);
		if (this.retryListener != null) {
			interceptor.setRetryListener(this.retryListener);
		}
		return interceptor;
	}

	@SuppressWarnings("unchecked")
	protected @Nullable Object recover(@Nullable Object[] args, Throwable cause) {
		MessageRecoverer messageRecoverer = getMessageRecoverer();
		Object arg = args[1];
		if (messageRecoverer == null) {
			this.logger.warn("Message(s) dropped on recovery: " + arg, cause);
		}
		else if (arg instanceof Message message) {
			messageRecoverer.recover(message, cause);
		}
		else if (arg instanceof List && messageRecoverer instanceof MessageBatchRecoverer messageBatchRecoverer) {
			messageBatchRecoverer.recover((List<Message>) arg, cause);
		}
		return null;
	}

	@Override
	public Class<?> getObjectType() {
		return StatelessRetryOperationsInterceptor.class;
	}

}
