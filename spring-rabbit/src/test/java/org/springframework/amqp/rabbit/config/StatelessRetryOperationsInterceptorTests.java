/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;

import org.aopalliance.intercept.MethodInvocation;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.retry.MessageBatchRecoverer;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.core.retry.RetryPolicy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

/**
 * Tests fpr {@link StatelessRetryOperationsInterceptor}.
 *
 * @author Stephane Nicoll
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
		Object[] arguments = new Object[] {"message"};
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
		Object[] arguments = new Object[] {"message"};
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
		Object[] arguments = new Object[] {"message"};
		MethodInvocation invocation = mock(MethodInvocation.class);
		given(invocation.getArguments()).willReturn(arguments);
		given(invocation.proceed()).willThrow(new IllegalStateException("initial"))
				.willThrow(new IllegalStateException("retry-1")).willThrow(LastException);
		StatelessRetryOperationsInterceptor noRecovererInterceptor = new StatelessRetryOperationsInterceptor(retryPolicy, null);
		assertThatException().isThrownBy(() -> noRecovererInterceptor.invoke(invocation))
				.isSameAs(LastException);
	}

	@Test
	void factoryBeanRecovererReceivesMessageFromSingleArgumentListener() throws Throwable {
		Message message = new Message("test".getBytes(), new MessageProperties());
		MessageRecoverer messageRecoverer = mock();
		Exception lastException = new IllegalStateException("retry-1");
		MethodInvocation invocation = mock(MethodInvocation.class);
		given(invocation.getArguments()).willReturn(new Object[] {message});
		given(invocation.proceed()).willThrow(new IllegalStateException("initial")).willThrow(lastException);
		assertThat(createInterceptorViaFactoryBean(messageRecoverer).invoke(invocation)).isNull();
		then(messageRecoverer).should().recover(message, lastException);
	}

	@Test
	void factoryBeanRecovererReceivesMessageFromAnyArgumentPosition() throws Throwable {
		Message message = new Message("test".getBytes(), new MessageProperties());
		MessageRecoverer messageRecoverer = mock();
		Exception lastException = new IllegalStateException("retry-1");
		MethodInvocation invocation = mock(MethodInvocation.class);
		given(invocation.getArguments()).willReturn(new Object[] {"amqpMessage", message, null});
		given(invocation.proceed()).willThrow(new IllegalStateException("initial")).willThrow(lastException);
		assertThat(createInterceptorViaFactoryBean(messageRecoverer).invoke(invocation)).isNull();
		then(messageRecoverer).should().recover(message, lastException);
	}

	@Test
	void factoryBeanRecovererReceivesBatchFromSingleArgumentListener() throws Throwable {
		List<Message> messages = List.of(new Message("test".getBytes(), new MessageProperties()));
		MessageBatchRecoverer messageRecoverer = mock();
		Exception lastException = new IllegalStateException("retry-1");
		MethodInvocation invocation = mock(MethodInvocation.class);
		given(invocation.getArguments()).willReturn(new Object[] {messages});
		given(invocation.proceed()).willThrow(new IllegalStateException("initial")).willThrow(lastException);
		assertThat(createInterceptorViaFactoryBean(messageRecoverer).invoke(invocation)).isNull();
		then(messageRecoverer).should().recover(messages, lastException);
	}

	private static StatelessRetryOperationsInterceptor createInterceptorViaFactoryBean(
			MessageRecoverer messageRecoverer) {

		StatelessRetryOperationsInterceptorFactoryBean factoryBean =
				new StatelessRetryOperationsInterceptorFactoryBean();
		factoryBean.setRetryPolicy(RetryPolicy.builder().maxRetries(1).delay(Duration.ZERO).build());
		factoryBean.setMessageRecoverer(messageRecoverer);
		return factoryBean.getObject();
	}

	private StatelessRetryOperationsInterceptor createInterceptor(@Nullable RetryPolicy retryPolicy) {
		return new StatelessRetryOperationsInterceptor(retryPolicy, this.recoverer);
	}

}
