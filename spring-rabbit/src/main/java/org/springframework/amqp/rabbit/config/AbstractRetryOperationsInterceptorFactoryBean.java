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

import org.aopalliance.aop.Advice;

import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.retry.RetryOperations;

/**
 * Convenient base class for interceptor factories.
 *
 * @author Dave Syer
 *
 */
public abstract class AbstractRetryOperationsInterceptorFactoryBean implements FactoryBean<Advice> {

	private MessageRecoverer messageRecoverer;

	private RetryOperations retryTemplate;

	public void setRetryOperations(RetryOperations retryTemplate) {
		this.retryTemplate = retryTemplate;
	}

	public void setMessageRecoverer(MessageRecoverer messageRecoverer) {
		this.messageRecoverer = messageRecoverer;
	}

	protected RetryOperations getRetryOperations() {
		return this.retryTemplate;
	}

	protected MessageRecoverer getMessageRecoverer() {
		return this.messageRecoverer;
	}

	public boolean isSingleton() {
		return true;
	}

}
