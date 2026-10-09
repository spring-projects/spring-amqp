/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import java.time.Duration;
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
 * @author Artem Bilan
 *
 * @since 4.0
 */
public final class StatelessRetryOperationsInterceptor implements MethodInterceptor {

	private final RetryOperations retryOperations;

	private final @Nullable BiFunction<@Nullable Object[], Throwable, @Nullable Object> recoverer;

	StatelessRetryOperationsInterceptor(@Nullable RetryPolicy retryPolicy,
			@Nullable BiFunction<@Nullable Object[], Throwable, @Nullable Object> recoverer) {
		this.retryOperations =
				new RetryTemplate(retryPolicy != null ? retryPolicy : RetryPolicy.builder().delay(Duration.ZERO).build());
		this.recoverer = recoverer;
	}

	@Override
	public @Nullable Object invoke(final MethodInvocation invocation) throws Throwable {
		try {
			return this.retryOperations.<@Nullable Object>execute(invocation::proceed);
		}
		catch (RetryException ex) {
			if (this.recoverer != null) {
				return this.recoverer.apply(invocation.getArguments(), ex.getCause());
			}
			throw ex.getCause();
		}
	}

}
