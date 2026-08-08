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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.aopalliance.intercept.MethodInvocation;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryListener;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryState;
import org.springframework.core.retry.Retryable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

/**
 * Tests for {@link StatelessRetryOperationsInterceptor}.
 *
 * @author Stephane Nicoll
 * @author Jun Cho
 */
class StatelessRetryOperationsInterceptorTests {

	private final @Nullable BiFunction<@Nullable Object[], Throwable, @Nullable Object> recoverer = mock();

	@Test
	void invokeWithSuccessOnFirstInvocation() throws Throwable {
		MethodInvocation invocation = mock(MethodInvocation.class);
		given(invocation.proceed()).willReturn("hello");
		assertThat(createInterceptor(null).invoke(invocation)).isEqualTo("hello");
		then(invocation).should(times(1)).proceed();
		then(this.recoverer).shouldHaveNoInteractions();
	}

	@Test
	void invokeWithFailuresNotExhaustingRetries() throws Throwable {
		RetryPolicy retryPolicy = RetryPolicy.builder().maxRetries(2).delay(Duration.ZERO).build();
		MethodInvocation invocation = mock(MethodInvocation.class);
		given(invocation.proceed()).willThrow(new IllegalStateException("initial"))
				.willThrow(new IllegalStateException("retry-1")).willReturn("hello");
		assertThat(createInterceptor(retryPolicy).invoke(invocation)).isEqualTo("hello");
		then(invocation).should(times(3)).proceed();
		then(this.recoverer).shouldHaveNoInteractions();
	}

	@Test
	void invokeWithFailuresExhaustingRetriesReturnsResultFromRecoverer() throws Throwable {
		RetryPolicy retryPolicy = RetryPolicy.builder().maxRetries(2).delay(Duration.ZERO).build();
		Exception lastException = new IllegalStateException("retry-2");
		Object[] arguments = new Object[] { "message" };
		MethodInvocation invocation = mock(MethodInvocation.class);
		given(invocation.getArguments()).willReturn(arguments);
		given(invocation.proceed()).willThrow(new IllegalStateException("initial"))
				.willThrow(new IllegalStateException("retry-1")).willThrow(lastException);
		ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
		given(this.recoverer.apply(eq(arguments), throwableCaptor.capture())).willReturn("recovered");
		assertThat(createInterceptor(retryPolicy).invoke(invocation)).isEqualTo("recovered");
		then(invocation).should(times(3)).proceed();
		assertThat(throwableCaptor.getValue()).isSameAs(lastException);
	}

	@Test
	void invokeWithFailuresExhaustingRetriesThrowsResultFromRecoverer() throws Throwable {
		RetryPolicy retryPolicy = RetryPolicy.builder().maxRetries(2).delay(Duration.ZERO).build();
		Exception recovererException = new IllegalStateException("failed");
		Object[] arguments = new Object[] { "message" };
		MethodInvocation invocation = mock(MethodInvocation.class);
		given(invocation.getArguments()).willReturn(arguments);
		given(invocation.proceed()).willThrow(new IllegalStateException("initial"))
				.willThrow(new IllegalStateException("retry-1")).willThrow(new IllegalStateException("retry-2"));
		given(this.recoverer.apply(eq(arguments), any())).willThrow(recovererException);
		assertThatException().isThrownBy(() -> createInterceptor(retryPolicy).invoke(invocation))
				.isEqualTo(recovererException);
		then(invocation).should(times(3)).proceed();
	}

	@Test
	void invokeWithFailuresExhaustingRetriesAndNoRecovererThrowsLastException() throws Throwable {
		RetryPolicy retryPolicy = RetryPolicy.builder().maxRetries(2).delay(Duration.ZERO).build();
		Exception LastException = new IllegalStateException("retry-2");
		Object[] arguments = new Object[] { "message" };
		MethodInvocation invocation = mock(MethodInvocation.class);
		given(invocation.getArguments()).willReturn(arguments);
		given(invocation.proceed()).willThrow(new IllegalStateException("initial"))
				.willThrow(new IllegalStateException("retry-1")).willThrow(LastException);
		StatelessRetryOperationsInterceptor noRecovererInterceptor = new StatelessRetryOperationsInterceptor(retryPolicy, null);
		assertThatException().isThrownBy(() -> noRecovererInterceptor.invoke(invocation))
				.isSameAs(LastException);
	}

	@Test
	void invokeWithRetryListenerInvokesCallbacks() throws Throwable {
		RetryPolicy retryPolicy = RetryPolicy.builder().maxRetries(2).delay(Duration.ZERO).build();
		Object[] arguments = new Object[] { "message" };
		MethodInvocation invocation = mock(MethodInvocation.class);
		given(invocation.getArguments()).willReturn(arguments);
		given(invocation.proceed()).willThrow(new IllegalStateException("initial"))
				.willThrow(new IllegalStateException("retry-1")).willThrow(new IllegalStateException("retry-2"));
		given(this.recoverer.apply(eq(arguments), any())).willReturn("recovered");
		AtomicInteger beforeRetryCalls = new AtomicInteger();
		AtomicInteger retryFailureCalls = new AtomicInteger();
		AtomicInteger exhaustionCalls = new AtomicInteger();
		StatelessRetryOperationsInterceptor interceptor = createInterceptor(retryPolicy);
		interceptor.setRetryListener(new RetryListener() {

			@Override
			public void beforeRetry(RetryPolicy policy, Retryable<?> retryable, RetryState retryState) {
				beforeRetryCalls.incrementAndGet();
			}

			@Override
			public void onRetryFailure(RetryPolicy policy, Retryable<?> retryable, Throwable throwable) {
				retryFailureCalls.incrementAndGet();
			}

			@Override
			public void onRetryPolicyExhaustion(RetryPolicy policy, Retryable<?> retryable,
					RetryException retryException) {

				exhaustionCalls.incrementAndGet();
			}

		});
		assertThat(interceptor.invoke(invocation)).isEqualTo("recovered");
		assertThat(beforeRetryCalls).hasValue(2);
		assertThat(retryFailureCalls).hasValue(2);
		assertThat(exhaustionCalls).hasValue(1);
	}

	@Test
	void invokeWithRetryListenerAppliedViaFactoryBean() throws Throwable {
		AtomicInteger exhaustionCalls = new AtomicInteger();
		StatelessRetryOperationsInterceptorFactoryBean factoryBean =
				new StatelessRetryOperationsInterceptorFactoryBean();
		factoryBean.setRetryPolicy(RetryPolicy.builder().maxRetries(1).delay(Duration.ZERO).build());
		factoryBean.setRetryListener(new RetryListener() {

			@Override
			public void onRetryPolicyExhaustion(RetryPolicy policy, Retryable<?> retryable,
					RetryException retryException) {

				exhaustionCalls.incrementAndGet();
			}

		});
		MethodInvocation invocation = mock(MethodInvocation.class);
		given(invocation.getArguments()).willReturn(new Object[] { "channel", "message" });
		given(invocation.proceed()).willThrow(new IllegalStateException("initial"))
				.willThrow(new IllegalStateException("retry-1"));
		assertThat(factoryBean.getObject().invoke(invocation)).isNull();
		assertThat(exhaustionCalls).hasValue(1);
	}

	private StatelessRetryOperationsInterceptor createInterceptor(@Nullable RetryPolicy retryPolicy) {
		return new StatelessRetryOperationsInterceptor(retryPolicy, this.recoverer);
	}

}
