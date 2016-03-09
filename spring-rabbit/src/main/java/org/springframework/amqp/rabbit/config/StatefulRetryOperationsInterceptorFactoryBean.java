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

package org.springframework.amqp.rabbit.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerExecutionException;
import org.springframework.amqp.rabbit.retry.MessageKeyGenerator;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.NewMessageIdentifier;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.interceptor.MethodArgumentsKeyGenerator;
import org.springframework.retry.interceptor.MethodInvocationRecoverer;
import org.springframework.retry.interceptor.NewMethodArgumentsIdentifier;
import org.springframework.retry.interceptor.StatefulRetryOperationsInterceptor;
import org.springframework.retry.support.RetryTemplate;

/**
 * Convenient factory bean for creating a stateful retry interceptor for use in a message listener container, giving you
 * a large amount of control over the behaviour of a container when a listener fails. To control the number of retry
 * attempt or the backoff in between attempts, supply a customized {@link RetryTemplate}. Stateful retry is appropriate
 * if your listener is using a transactional resource that needs to be rollback on an exception (e.g. a stateful
 * connection to a back end server). JPA is the canonical example. The semantics of stateful retry mean that a listener
 * exception is propagated to the container, so that it can force a rollback. When the message is redelivered it has to
 * be recognised (hence the {@link MessageKeyGenerator} strategy), and when the retry attempts are exhausted it will be
 * processed using a {@link MessageRecoverer} if one is provided, in a new transaction. If a recoverer is not provided
 * the message will be logged and dropped.
 *
 * @see RetryOperations#execute(org.springframework.retry.RetryCallback, org.springframework.retry.RecoveryCallback,
 * org.springframework.retry.RetryState)
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public class StatefulRetryOperationsInterceptorFactoryBean extends AbstractRetryOperationsInterceptorFactoryBean {

	private static Log logger = LogFactory.getLog(StatefulRetryOperationsInterceptorFactoryBean.class);

	private MessageKeyGenerator messageKeyGenerator;

	private NewMessageIdentifier newMessageIdentifier;

	public void setMessageKeyGenerator(MessageKeyGenerator messageKeyGeneretor) {
		this.messageKeyGenerator = messageKeyGeneretor;
	}

	public void setNewMessageIdentifier(NewMessageIdentifier newMessageIdentifier) {
		this.newMessageIdentifier = newMessageIdentifier;
	}

	public StatefulRetryOperationsInterceptor getObject() {

		StatefulRetryOperationsInterceptor retryInterceptor = new StatefulRetryOperationsInterceptor();
		RetryOperations retryTemplate = getRetryOperations();
		if (retryTemplate == null) {
			retryTemplate = new RetryTemplate();
		}
		retryInterceptor.setRetryOperations(retryTemplate);

		retryInterceptor.setNewItemIdentifier(new NewMethodArgumentsIdentifier() {
			public boolean isNew(Object[] args) {
				Message message = (Message) args[1];
				if (StatefulRetryOperationsInterceptorFactoryBean.this.newMessageIdentifier == null) {
					return !message.getMessageProperties().isRedelivered();
				}
				else {
					return StatefulRetryOperationsInterceptorFactoryBean.this.newMessageIdentifier.isNew(message);
				}
			}
		});

		final MessageRecoverer messageRecoverer = getMessageRecoverer();
		retryInterceptor.setRecoverer(new MethodInvocationRecoverer<Void>() {
			public Void recover(Object[] args, Throwable cause) {
				Message message = (Message) args[1];
				if (messageRecoverer == null) {
					logger.warn("Message dropped on recovery: " + message, cause);
				} else {
					messageRecoverer.recover(message, cause);
				}
				// This is actually a normal outcome. It means the recovery was successful, but we don't want to consume
				// any more messages until the acks and commits are sent for this (problematic) message...
				throw new ImmediateAcknowledgeAmqpException("Recovered message forces ack (if ack mode requires it): "
						+ message, cause);
			}
		});

		retryInterceptor.setKeyGenerator(new MethodArgumentsKeyGenerator() {
			public Object getKey(Object[] args) {
				Message message = (Message) args[1];
				if (StatefulRetryOperationsInterceptorFactoryBean.this.messageKeyGenerator == null) {
					String messageId = message.getMessageProperties().getMessageId();
					if (messageId == null) {
						throw new FatalListenerExecutionException(
								"Illegal null id in message. Failed to manage retry for message: " + message);
					}
					return messageId;
				}
				else {
					return StatefulRetryOperationsInterceptorFactoryBean.this.messageKeyGenerator.getKey(message);
				}
			}
		});

		return retryInterceptor;

	}

	public Class<?> getObjectType() {
		return StatefulRetryOperationsInterceptor.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

}
