/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.MessageBatchRecoverer;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
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
 * @author Seonwoo Jung
 *
 * @see RetryOperations#execute(Retryable)
 */
public class StatelessRetryOperationsInterceptorFactoryBean extends AbstractRetryOperationsInterceptorFactoryBean {

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	@Override
	public StatelessRetryOperationsInterceptor getObject() {
		return new StatelessRetryOperationsInterceptor(getRetryPolicy(), this::recover);
	}

	@SuppressWarnings("unchecked")
	protected @Nullable Object recover(@Nullable Object[] args, Throwable cause) {
		MessageRecoverer messageRecoverer = getMessageRecoverer();
		Object arg = findMessageArgument(args);
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

	/**
	 * Find the first {@link Message} or {@link List} argument of the advised listener method.
	 * The position depends on the container: the AMQP 0-9-1 containers pass the message after
	 * the {@code Channel}, but other containers may advise, for example,
	 * {@link org.springframework.amqp.core.MessageListener#onMessage(Message)} directly.
	 * @param args the arguments of the advised method.
	 * @return the message (or list of messages), or null if not found.
	 */
	private static @Nullable Object findMessageArgument(@Nullable Object[] args) {
		for (Object arg : args) {
			if (arg instanceof Message || arg instanceof List) {
				return arg;
			}
		}
		return null;
	}

	@Override
	public Class<?> getObjectType() {
		return StatelessRetryOperationsInterceptor.class;
	}

}
