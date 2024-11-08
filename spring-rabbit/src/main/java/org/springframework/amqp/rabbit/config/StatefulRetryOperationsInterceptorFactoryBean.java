/*
 * Copyright 2002-2024 the original author or authors.
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

import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.MessageBatchRecoverer;
import org.springframework.amqp.rabbit.retry.MessageKeyGenerator;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.NewMessageIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.interceptor.MethodArgumentsKeyGenerator;
import org.springframework.retry.interceptor.MethodInvocationRecoverer;
import org.springframework.retry.interceptor.NewMethodArgumentsIdentifier;
import org.springframework.retry.interceptor.StatefulRetryOperationsInterceptor;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

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
 * @author Dave Syer
 * @author Gary Russell
 * @author Ngoc Nhan
 *
 * @see RetryOperations#execute(org.springframework.retry.RetryCallback, org.springframework.retry.RecoveryCallback,
 * org.springframework.retry.RetryState)
 *
 */
public class StatefulRetryOperationsInterceptorFactoryBean extends AbstractRetryOperationsInterceptorFactoryBean {

	private static Log logger = LogFactory.getLog(StatefulRetryOperationsInterceptorFactoryBean.class);

	private MessageKeyGenerator messageKeyGenerator;

	private NewMessageIdentifier newMessageIdentifier;

	public void setMessageKeyGenerator(MessageKeyGenerator messageKeyGenerator) {
		this.messageKeyGenerator = messageKeyGenerator;
	}

	public void setNewMessageIdentifier(NewMessageIdentifier newMessageIdentifier) {
		this.newMessageIdentifier = newMessageIdentifier;
	}

	@Override
	public StatefulRetryOperationsInterceptor getObject() {

		StatefulRetryOperationsInterceptor retryInterceptor = new StatefulRetryOperationsInterceptor();
		RetryOperations retryTemplate = getRetryOperations();
		if (retryTemplate == null) {
			retryTemplate = new RetryTemplate();
		}
		retryInterceptor.setRetryOperations(retryTemplate);
		retryInterceptor.setNewItemIdentifier(createNewItemIdentifier());
		retryInterceptor.setRecoverer(createRecoverer());
		retryInterceptor.setKeyGenerator(createKeyGenerator());
		return retryInterceptor;

	}

	private NewMethodArgumentsIdentifier createNewItemIdentifier() {
		return args -> {
			Message message = argToMessage(args);
			if (StatefulRetryOperationsInterceptorFactoryBean.this.newMessageIdentifier == null) {
				return !message.getMessageProperties().isRedelivered();
			}

			return StatefulRetryOperationsInterceptorFactoryBean.this.newMessageIdentifier.isNew(message);
		};
	}

	@SuppressWarnings("unchecked")
	private MethodInvocationRecoverer<?> createRecoverer() {
		return (args, cause) -> {
			MessageRecoverer messageRecoverer = getMessageRecoverer();
			Object arg = args[1];
			if (messageRecoverer == null) {
				logger.warn("Message(s) dropped on recovery: " + arg, cause);
			}
			else if (arg instanceof Message msg) {
				messageRecoverer.recover(msg, cause);
			}
			else if (arg instanceof List && messageRecoverer instanceof MessageBatchRecoverer recoverer) {
				recoverer.recover((List<Message>) arg, cause);
			}
			// This is actually a normal outcome. It means the recovery was successful, but we don't want to consume
			// any more messages until the acks and commits are sent for this (problematic) message...
			throw new ImmediateAcknowledgeAmqpException("Recovered message forces ack (if ack mode requires it): "
					+ arg, cause);
		};
	}

	private MethodArgumentsKeyGenerator createKeyGenerator() {
		return args -> {
			Message message = argToMessage(args);
			Assert.notNull(message, "The 'args' must not convert to null");
			if (StatefulRetryOperationsInterceptorFactoryBean.this.messageKeyGenerator == null) {
				String messageId = message.getMessageProperties().getMessageId();
				if (messageId == null && message.getMessageProperties().isRedelivered()) {
					message.getMessageProperties().setFinalRetryForMessageWithNoId(true);
				}
				return messageId;
			}
			return StatefulRetryOperationsInterceptorFactoryBean.this.messageKeyGenerator.getKey(message);
		};
	}

	@Nullable
	private Message argToMessage(Object[] args) {
		Object arg = args[1];
		if (arg instanceof Message msg) {
			return msg;
		}
		if (arg instanceof List<?> list) {
			return (Message) list.get(0);
		}
		return null;
	}

	@Override
	public Class<?> getObjectType() {
		return StatefulRetryOperationsInterceptor.class;
	}

}
