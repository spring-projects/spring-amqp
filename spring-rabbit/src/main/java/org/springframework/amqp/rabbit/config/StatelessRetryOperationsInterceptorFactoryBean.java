/*
 * Copyright 2002-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
import org.springframework.retry.support.RetryTemplate;

/**
 * Convenient factory bean for creating a stateless retry interceptor for use in a message listener container, giving
 * you a large amount of control over the behaviour of a container when a listener fails. To control the number of retry
 * attempt or the backoff in between attempts, supply a customized {@link RetryTemplate}. Stateless retry is appropriate
 * if your listener can be called repeatedly between failures with no side effects. The semantics of stateless retry
 * mean that a listener exception is not propagated to the container until the retry attempts are exhausted. When the
 * retry attempts are exhausted it can be processed using a {@link MessageRecoverer} if one is provided, in the same
 * transaction (in which case no exception is propagated). If a recoverer is not provided the exception will be
 * propagated and the message may be redelivered if the channel is transactional.
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 * @see RetryOperations#execute(org.springframework.retry.RetryCallback, org.springframework.retry.RecoveryCallback)
 */
public class StatelessRetryOperationsInterceptorFactoryBean extends AbstractRetryOperationsInterceptorFactoryBean {

	private static Log logger = LogFactory.getLog(StatelessRetryOperationsInterceptorFactoryBean.class);

	@Override
	public RetryOperationsInterceptor getObject() {

		RetryOperationsInterceptor retryInterceptor = new RetryOperationsInterceptor();
		RetryOperations retryTemplate = getRetryOperations();
		if (retryTemplate == null) {
			retryTemplate = new RetryTemplate();
		}
		retryInterceptor.setRetryOperations(retryTemplate);

		final MessageRecoverer messageRecoverer = getMessageRecoverer();
		retryInterceptor.setRecoverer((args, cause) -> {
			Message message = (Message) args[1];
			if (messageRecoverer == null) {
				logger.warn("Message dropped on recovery: " + message, cause);
			}
			else {
				messageRecoverer.recover(message, cause);
			}
			return null;
		});

		return retryInterceptor;

	}

	@Override
	public Class<?> getObjectType() {
		return RetryOperationsInterceptor.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

}
