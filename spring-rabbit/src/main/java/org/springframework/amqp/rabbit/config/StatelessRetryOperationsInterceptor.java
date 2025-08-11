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

import java.util.function.BiFunction;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.jspecify.annotations.Nullable;

import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryOperations;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;

/**
 * {@link MethodInterceptor} implementation that retries method invocation with a
 * {@link RetryOperations}.
 *
 * @author Juergen Hoeller
 * @author Stephane Nicoll
 */
public final class StatelessRetryOperationsInterceptor implements MethodInterceptor {

	private final RetryOperations retryOperations;

	private final @Nullable BiFunction<@Nullable Object[], Throwable, @Nullable Object> recoverer;

	StatelessRetryOperationsInterceptor(@Nullable RetryPolicy retryPolicy,
			@Nullable BiFunction<@Nullable Object[], Throwable, @Nullable Object> recoverer) {
		this.retryOperations = new RetryTemplate((retryPolicy != null) ? retryPolicy : RetryPolicy.builder().build());
		this.recoverer = recoverer;
	}

	@Override
	public @Nullable Object invoke(final MethodInvocation invocation) throws Throwable {
		try {
			return this.retryOperations.execute(invocation::proceed);
		}
		catch (RetryException ex) {
			if (this.recoverer != null) {
				return this.recoverer.apply(invocation.getArguments(), ex.getCause());
			}
			throw ex.getCause();
		}
	}

}
