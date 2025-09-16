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

import java.util.List;
import java.util.function.Predicate;

import org.aopalliance.intercept.MethodInvocation;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.MapAssert;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.config.StatefulRetryOperationsInterceptor.RetryState;
import org.springframework.amqp.rabbit.retry.MessageBatchRecoverer;
import org.springframework.amqp.rabbit.retry.MessageKeyGenerator;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.NewMessageIdentifier;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

/**
 * Tests for {@link StatefulRetryOperationsInterceptor}.
 *
 * @author Stephane Nicoll
 */
class StatefulRetryOperationsInterceptorTests {

	private final Message message;

	private final MethodInvocation invocation;

	private final MessageKeyGenerator messageKeyGenerator;

	private final NewMessageIdentifier newMessageIdentifier;

	StatefulRetryOperationsInterceptorTests() {
		this.message = mock();
		this.invocation = mock();
		given(this.invocation.getArguments()).willReturn(new Object[] {"dummy", this.message});
		this.messageKeyGenerator = mock();
		this.newMessageIdentifier = mock();
	}

	@Test
	void invokeWhenArgsHasNoArgument() {
		MethodInvocation invalidInvocation = mock();
		given(invalidInvocation.getArguments()).willReturn(new Object[] {});

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(null, null);
		assertThatIllegalArgumentException()
				.isThrownBy(() -> assertThat(interceptor.invoke(invalidInvocation)))
				.withMessage("Expected 2nd arguments to be a message, got []");
	}

	@Test
	void invokeWhenArgsHasSingleArgument() {
		MethodInvocation invalidInvocation = mock();
		given(invalidInvocation.getArguments()).willReturn(new Object[] {"dummy"});

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(null, null);
		assertThatIllegalArgumentException()
				.isThrownBy(() -> assertThat(interceptor.invoke(invalidInvocation)))
				.withMessage("Expected 2nd arguments to be a message, got [dummy]");
	}

	@Test
	void invokeWhenArgsHasSecondArgumentWithInvalidType() {
		MethodInvocation invalidInvocation = mock();
		given(invalidInvocation.getArguments()).willReturn(new Object[] {"dummy", "invalid"});

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(null, null);
		assertThatIllegalArgumentException()
				.isThrownBy(() -> assertThat(interceptor.invoke(invalidInvocation)))
				.withMessage("Expected 2nd arguments to be a message, got [dummy, invalid]");
	}

	@Test
	void invokeWhenKeyIsNull() throws Throwable {
		given(this.messageKeyGenerator.getKey(this.message)).willReturn(null);
		given(this.invocation.proceed()).willReturn("hello");

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(null, null);
		assertThat(interceptor.invoke(this.invocation)).isEqualTo("hello");
		assertThatCache(interceptor).isEmpty();
		then(this.invocation).should().proceed();
		then(this.messageKeyGenerator).should().getKey(this.message);
		then(this.newMessageIdentifier).shouldHaveNoInteractions();
	}

	@Test
	void invokeWhenKeyIsNullAndFailureDoesNotRecordRetryState() throws Throwable {
		given(this.messageKeyGenerator.getKey(this.message)).willReturn(null);
		given(this.invocation.proceed()).willReturn("hello");

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(null, null);
		Exception failure = new IllegalStateException("test");
		given(this.invocation.proceed()).willThrow(failure);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(failure);
		assertThatCache(interceptor).isEmpty();
		then(this.invocation).should().proceed();
		then(this.messageKeyGenerator).should().getKey(this.message);
		then(this.newMessageIdentifier).shouldHaveNoInteractions();
	}

	@Test
	void invokeWhenInitialInvocationSucceeds() throws Throwable {
		given(this.messageKeyGenerator.getKey(this.message)).willReturn("id");
		given(this.invocation.proceed()).willReturn("hello");

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(null, null);
		assertThat(interceptor.invoke(this.invocation)).isEqualTo("hello");
		assertThatCache(interceptor).isEmpty();
		then(this.invocation).should().proceed();
		then(this.messageKeyGenerator).should().getKey(this.message);
		then(this.newMessageIdentifier).shouldHaveNoInteractions();
	}

	@Test
	void invokeWhenInitialInvocationFailsCreateCacheEntry() throws Throwable {
		BackOffExecution backOffExecution = mock();
		BackOff backOff = mock();
		given(backOff.start()).willReturn(backOffExecution);
		RetryPolicy retryPolicy = RetryPolicy.builder().backOff(backOff).build();
		Exception failure = new IllegalStateException("test");
		given(this.messageKeyGenerator.getKey(this.message)).willReturn("id");
		given(this.invocation.proceed()).willThrow(failure);

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(retryPolicy, null);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(failure);
		assertThatCache(interceptor).containsEntry("id", new RetryState(backOffExecution, failure));
		then(backOff).should().start();
		then(backOffExecution).shouldHaveNoInteractions();
	}

	@Test
	void invokeWhenRetryFailsUpdatesCacheEntry() throws Throwable {
		BackOffExecution backOffExecution = mock();
		given(backOffExecution.nextBackOff()).willReturn(0L);
		BackOff backOff = mock();
		given(backOff.start()).willReturn(backOffExecution);
		RetryPolicy retryPolicy = RetryPolicy.builder().backOff(backOff).build();
		Exception initialAttemptFailure = new IllegalStateException("test");
		Exception retryFailure = new IllegalStateException("retry-1");
		given(this.messageKeyGenerator.getKey(this.message)).willReturn("id");
		given(this.invocation.proceed()).willThrow(initialAttemptFailure).willThrow(retryFailure);

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(retryPolicy, null);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(initialAttemptFailure);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(retryFailure);
		assertThatCache(interceptor).containsEntry("id", new RetryState(backOffExecution, retryFailure));
		then(this.invocation).should(times(2)).proceed();
		then(backOff).should().start();
		then(backOffExecution).should().nextBackOff();
	}

	@Test
	void invokeWhenRetrySucceedsCleansCache() throws Throwable {
		BackOffExecution backOffExecution = mock();
		given(backOffExecution.nextBackOff()).willReturn(0L);
		BackOff backOff = mock();
		given(backOff.start()).willReturn(backOffExecution);
		RetryPolicy retryPolicy = RetryPolicy.builder().backOff(backOff).build();
		Exception initialAttemptFailure = new IllegalStateException("test");
		given(this.messageKeyGenerator.getKey(this.message)).willReturn("id");
		given(this.invocation.proceed()).willThrow(initialAttemptFailure).willReturn("hello");

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(retryPolicy, null);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(initialAttemptFailure);
		assertThat(interceptor.invoke(this.invocation)).isEqualTo("hello");
		assertThatCache(interceptor).isEmpty();
		then(this.invocation).should(times(2)).proceed();
		then(backOff).should().start();
		then(backOffExecution).should().nextBackOff();
	}

	@Test
	void invokeWhenRetryPolicyShouldNotRetryThrowsImmediateAcknowledgeAmqpExceptionAndCleansCache() throws Throwable {
		BackOffExecution backOffExecution = mock();
		given(backOffExecution.nextBackOff()).willReturn(0L);
		BackOff backOff = mock();
		given(backOff.start()).willReturn(backOffExecution);
		Exception initialAttemptFailure = new IllegalStateException("test");
		Predicate<Throwable> shouldRetry = mock();
		given(shouldRetry.test(initialAttemptFailure)).willReturn(false);
		RetryPolicy retryPolicy = RetryPolicy.builder().predicate(shouldRetry).backOff(backOff).build();
		given(this.messageKeyGenerator.getKey(this.message)).willReturn("id");
		given(this.invocation.proceed()).willThrow(initialAttemptFailure);

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(retryPolicy, null);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(initialAttemptFailure);
		assertThatExceptionOfType(ImmediateAcknowledgeAmqpException.class)
				.isThrownBy(() -> interceptor.invoke(this.invocation)).havingCause().isEqualTo(initialAttemptFailure);
		assertThatCache(interceptor).isEmpty();
		then(this.invocation).should(times(1)).proceed();
		then(backOff).should().start();
		then(backOffExecution).should().nextBackOff();
	}

	@Test
	void invokesWhenRetriesAreExhaustedThrowsImmediateAcknowledgeAmqpExceptionAndCleansCache() throws Throwable {
		BackOffExecution backOffExecution = mock();
		given(backOffExecution.nextBackOff()).willReturn(0L).willReturn(0L).willReturn(BackOffExecution.STOP);
		BackOff backOff = mock();
		given(backOff.start()).willReturn(backOffExecution);
		RetryPolicy retryPolicy = RetryPolicy.builder().backOff(backOff).build();
		Exception initialAttemptFailure = new IllegalStateException("test");
		Exception retryFailure = new IllegalStateException("retry-1");
		Exception finalFailure = new IllegalStateException("finalFailure");
		given(this.messageKeyGenerator.getKey(this.message)).willReturn("id");
		given(this.invocation.proceed()).willThrow(initialAttemptFailure).willThrow(retryFailure).willThrow(finalFailure);

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(retryPolicy, null);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(initialAttemptFailure);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(retryFailure);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(finalFailure);
		assertThatExceptionOfType(ImmediateAcknowledgeAmqpException.class)
				.isThrownBy(() -> interceptor.invoke(this.invocation)).havingCause().isEqualTo(finalFailure);
		assertThatCache(interceptor).isEmpty();
		then(this.invocation).should(times(3)).proceed();
		then(backOff).should().start();
		then(backOffExecution).should(times(3)).nextBackOff();
	}

	@Test
	void invokeWhenRetryInvokesNewMessageIdentifier() throws Throwable {
		BackOffExecution backOffExecution = mock();
		given(backOffExecution.nextBackOff()).willReturn(0L);
		BackOff backOff = mock();
		given(backOff.start()).willReturn(backOffExecution);
		RetryPolicy retryPolicy = RetryPolicy.builder().backOff(backOff).build();
		Exception initialAttemptFailure = new IllegalStateException("test");
		Exception retryFailure = new IllegalStateException("retry-1");
		given(this.messageKeyGenerator.getKey(this.message)).willReturn("id");
		given(this.newMessageIdentifier.isNew(this.message)).willReturn(false);
		given(this.invocation.proceed()).willThrow(initialAttemptFailure).willThrow(retryFailure);

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(retryPolicy, null);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(initialAttemptFailure);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(retryFailure);
		assertThatCache(interceptor).containsEntry("id", new RetryState(backOffExecution, retryFailure));
		then(this.invocation).should(times(2)).proceed();
		then(backOff).should().start();
		then(backOffExecution).should().nextBackOff();
		then(this.newMessageIdentifier).should().isNew(this.message);
	}

	@Test
	void invokeWhenRetryInvokesNewMessageIdentifierAndNewMessageResetRetryState() throws Throwable {
		BackOffExecution backOffExecution1 = mock();
		BackOffExecution backOffExecution2 = mock();
		BackOff backOff = mock();
		given(backOff.start()).willReturn(backOffExecution1).willReturn(backOffExecution2);
		RetryPolicy retryPolicy = RetryPolicy.builder().backOff(backOff).build();
		Exception initialAttemptFailure = new IllegalStateException("test");
		Exception retryFailure = new IllegalStateException("retry-1");
		given(this.messageKeyGenerator.getKey(this.message)).willReturn("id");
		given(this.newMessageIdentifier.isNew(this.message)).willReturn(true);
		given(this.invocation.proceed()).willThrow(initialAttemptFailure).willThrow(retryFailure);

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(retryPolicy, null);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(initialAttemptFailure);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(retryFailure);
		then(this.newMessageIdentifier).should().isNew(this.message);
		assertThatCache(interceptor).containsEntry("id", new RetryState(backOffExecution2, retryFailure));
		then(this.invocation).should(times(2)).proceed();
		then(backOff).should(times((2))).start();
		then(backOffExecution1).shouldHaveNoInteractions();
		then(backOffExecution2).shouldHaveNoInteractions();
	}

	@Test
	void invokeWhenRetryInvokesNewMessageIdentifierAndNewMessageReusedRetryState() throws Throwable {
		BackOffExecution backOffExecution1 = mock();
		BackOffExecution backOffExecution2 = mock();
		given(backOffExecution2.nextBackOff()).willReturn(0L);
		BackOff backOff = mock();
		given(backOff.start()).willReturn(backOffExecution1).willReturn(backOffExecution2);
		RetryPolicy retryPolicy = RetryPolicy.builder().backOff(backOff).build();
		Exception initialAttemptFailure = new IllegalStateException("test");
		Exception retryFailure = new IllegalStateException("retry-1");
		Exception secondRetryFailure = new IllegalStateException("retry-2");
		given(this.messageKeyGenerator.getKey(this.message)).willReturn("id");
		given(this.newMessageIdentifier.isNew(this.message)).willReturn(true).willReturn(false);
		given(this.invocation.proceed()).willThrow(initialAttemptFailure).willThrow(retryFailure).willThrow(secondRetryFailure);

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(retryPolicy, null);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(initialAttemptFailure);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(retryFailure);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(secondRetryFailure);
		then(this.newMessageIdentifier).should(times(2)).isNew(this.message);
		assertThatCache(interceptor).containsEntry("id", new RetryState(backOffExecution2, secondRetryFailure));
		then(this.invocation).should(times(3)).proceed();
		then(backOff).should(times((2))).start();
		then(backOffExecution1).shouldHaveNoInteractions();
		then(backOffExecution2).should().nextBackOff();
	}

	@Test
	void invokesWhenRetriesAreExhaustedInvokesMessageRecoverer() throws Throwable {
		Exception failure = new IllegalStateException("test");
		given(this.messageKeyGenerator.getKey(this.message)).willReturn("id");
		given(this.invocation.proceed()).willThrow(failure);
		MessageRecoverer messageRecoverer = mock();

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(createNoRetryPolicy(), messageRecoverer);
		assertThatException().isThrownBy(() -> interceptor.invoke(this.invocation)).isEqualTo(failure);
		assertThatExceptionOfType(ImmediateAcknowledgeAmqpException.class)
				.isThrownBy(() -> interceptor.invoke(this.invocation)).havingCause().isEqualTo(failure);
		then(messageRecoverer).should().recover(this.message, failure);
		then(messageRecoverer).shouldHaveNoMoreInteractions();
		assertThatCache(interceptor).isEmpty();
		then(this.invocation).should(times(1)).proceed();
	}

	@Test
	void invokesWhenRetriesAreExhaustedInvokesMessageBatchRecoverer() throws Throwable {
		List<Message> messages = List.of(this.message);
		MethodInvocation listInvocation = mock();
		given(listInvocation.getArguments()).willReturn(new Object[] {"dummy", messages});
		Exception failure = new IllegalStateException("test");
		given(this.messageKeyGenerator.getKey(this.message)).willReturn("id");
		given(listInvocation.proceed()).willThrow(failure);
		MessageBatchRecoverer messageRecoverer = mock();

		StatefulRetryOperationsInterceptor interceptor = createInterceptor(createNoRetryPolicy(), messageRecoverer);
		assertThatException().isThrownBy(() -> interceptor.invoke(listInvocation)).isEqualTo(failure);
		assertThatExceptionOfType(ImmediateAcknowledgeAmqpException.class)
				.isThrownBy(() -> interceptor.invoke(listInvocation)).havingCause().isEqualTo(failure);
		then(messageRecoverer).should().recover(messages, failure);
		then(messageRecoverer).shouldHaveNoMoreInteractions();
		assertThatCache(interceptor).isEmpty();
		then(listInvocation).should(times(1)).proceed();
	}

	private RetryPolicy createNoRetryPolicy() {
		BackOffExecution backOffExecution = mock();
		given(backOffExecution.nextBackOff()).willReturn(BackOffExecution.STOP);
		BackOff backOff = mock();
		given(backOff.start()).willReturn(backOffExecution);
		return RetryPolicy.builder().backOff(backOff).build();
	}

	private MapAssert<String, RetryState> assertThatCache(StatefulRetryOperationsInterceptor interceptor) {
		return assertThat(interceptor)
				.extracting("retryStateCache")
				.asInstanceOf(InstanceOfAssertFactories.map(String.class, RetryState.class));
	}

	private StatefulRetryOperationsInterceptor createInterceptor(@Nullable RetryPolicy retryPolicy,
			@Nullable MessageRecoverer messageRecoverer) {

		return new StatefulRetryOperationsInterceptor(this.messageKeyGenerator,
				this.newMessageIdentifier, retryPolicy, messageRecoverer, 100);
	}

}
