/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.rabbit.stream.retry;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler.Context;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.config.StatelessRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.core.retry.RetryOperations;
import org.springframework.rabbit.stream.listener.StreamListenerContainer;
import org.springframework.util.Assert;

/**
 * Convenient factory bean for creating a stateless retry interceptor for use in a
 * {@link StreamListenerContainer} when consuming native stream messages, giving you a
 * large amount of control over the behavior of a container when a listener fails. To
 * control the number of retry attempts or the backoff in between attempts, supply a
 * customized {@link RetryOperations}. Stateless retry is appropriate if your listener can
 * be called repeatedly between failures with no side effects. The semantics of stateless
 * retry mean that a listener exception is not propagated to the container until the retry
 * attempts are exhausted. When the retry attempts are exhausted it can be processed using
 * a {@link StreamMessageRecoverer} if one is provided.
 *
 * @author Gary Russell
 * @author Stephane Nicoll
 */
public class StreamRetryOperationsInterceptorFactoryBean extends StatelessRetryOperationsInterceptorFactoryBean {

	@Override
	protected @Nullable Object recover(@Nullable Object[] args, Throwable cause) {
		StreamMessageRecoverer messageRecoverer = (StreamMessageRecoverer) getMessageRecoverer();
		Object arg = args[0];
		if (arg instanceof org.springframework.amqp.core.Message) {
			return super.recover(args, cause);
		}
		else {
			if (messageRecoverer == null) {
				this.logger.warn("Message(s) dropped on recovery: " + arg, cause);
			}
			else {
				Message message = (Message) arg;
				Context context = (Context) args[1];
				Assert.notNull(message, "Message must not be null");
				Assert.notNull(context, "Context must not be null");
				messageRecoverer.recover(message, context, cause);
			}
			return null;
		}
	}

	/**
	 * Set a {@link StreamMessageRecoverer} to call when retries are exhausted.
	 * @param messageRecoverer the recoverer.
	 */
	public void setStreamMessageRecoverer(StreamMessageRecoverer messageRecoverer) {
		super.setMessageRecoverer(messageRecoverer);
	}

	@Override
	public void setMessageRecoverer(MessageRecoverer messageRecoverer) {
		throw new UnsupportedOperationException("Use setStreamMessageRecoverer() instead");
	}

}
