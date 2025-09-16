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

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.retry.MessageKeyGenerator;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.NewMessageIdentifier;
import org.springframework.core.retry.RetryPolicy;

/**
 * Convenient factory bean for creating a stateful retry interceptor for use in a message listener container, giving you
 * a large amount of control over the behavior of a container when a listener fails. To control the number of retry
 * attempts or the backoff in between attempts, supply a customized {@link RetryPolicy}. Stateful retry is appropriate
 * if your listener is using a transactional resource that needs to be rolled back on an exception (e.g., a stateful
 * connection to a back end server). JPA is the canonical example. The semantics of stateful retry mean that a listener
 * exception is propagated to the container so that it can force a rollback. When the message is redelivered it has to
 * be recognised (hence the {@link MessageKeyGenerator} strategy), and when the retry attempts are exhausted it will be
 * processed using a {@link MessageRecoverer} if one is provided, in a new transaction. If a recoverer is not provided,
 * the message will be logged and dropped.
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Artem Bilan
 * @author Stephane Nicoll
 *
 * @see RetryPolicy#shouldRetry(Throwable)
 */
public class StatefulRetryOperationsInterceptorFactoryBean extends AbstractRetryOperationsInterceptorFactoryBean {

	private MessageKeyGenerator messageKeyGenerator =
			(message) -> {
				MessageProperties messageProperties = message.getMessageProperties();
				String messageId = messageProperties.getMessageId();
				if (messageId == null && Boolean.TRUE.equals(messageProperties.isRedelivered())) {
					messageProperties.setFinalRetryForMessageWithNoId(true);
				}
				return messageId;
			};

	private NewMessageIdentifier newMessageIdentifier =
			(message) -> Boolean.FALSE.equals(message.getMessageProperties().isRedelivered());

	private @Nullable Integer stateCacheSize;

	public void setMessageKeyGenerator(MessageKeyGenerator messageKeyGenerator) {
		this.messageKeyGenerator = messageKeyGenerator;
	}

	public void setNewMessageIdentifier(NewMessageIdentifier newMessageIdentifier) {
		this.newMessageIdentifier = newMessageIdentifier;
	}

	public void setStateCacheSize(int stateCacheSize) {
		this.stateCacheSize = stateCacheSize;
	}

	@Override
	public StatefulRetryOperationsInterceptor getObject() {
		return new StatefulRetryOperationsInterceptor(this.messageKeyGenerator,
				this.newMessageIdentifier, getRetryPolicy(), getMessageRecoverer(), this.stateCacheSize);
	}

	@Override
	public Class<?> getObjectType() {
		return StatefulRetryOperationsInterceptor.class;
	}

}
