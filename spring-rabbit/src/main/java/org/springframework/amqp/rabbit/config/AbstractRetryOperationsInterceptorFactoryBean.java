/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import org.aopalliance.aop.Advice;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.core.retry.RetryPolicy;

/**
 * Convenient base class for interceptor factories.
 *
 * @author Dave Syer
 * @author Stephane Nicoll
 *
 */
public abstract class AbstractRetryOperationsInterceptorFactoryBean implements FactoryBean<Advice> {

	private @Nullable MessageRecoverer messageRecoverer;

	private @Nullable RetryPolicy retryPolicy;

	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	public void setMessageRecoverer(MessageRecoverer messageRecoverer) {
		this.messageRecoverer = messageRecoverer;
	}

	protected @Nullable RetryPolicy getRetryPolicy() {
		return this.retryPolicy;
	}

	protected @Nullable MessageRecoverer getMessageRecoverer() {
		return this.messageRecoverer;
	}

}
