/*
 * Copyright 2014-present the original author or authors.
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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.retry.ImmediateRequeueMessageRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.aop.Pointcut;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 *
 */
public class RetryInterceptorBuilderSupportTests {

	@Test
	public void testBasic() {
		StatefulRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateful().build();

		assertThat(TestUtils.getPropertyValue(interceptor, "retryPolicy.backOff.maxAttempts")).isEqualTo(3L);
	}

	@Test
	public void testWithCustomRetryPolicy() {
		RetryPolicy retryPolicy = RetryPolicy.builder().maxRetries(2).build();
		StatefulRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateful()
				.retryPolicy(retryPolicy)
				.build();

		assertThat(TestUtils.getPropertyValue(interceptor, "retryPolicy.backOff.maxAttempts")).isEqualTo(2L);
		assertThat(TestUtils.getPropertyValue(interceptor, "retryPolicy")).isSameAs(retryPolicy);
	}

	@Test
	public void testWithMoreAttempts() {
		StatefulRetryOperationsInterceptor interceptor =
				RetryInterceptorBuilder.stateful()
						.maxRetries(5)
						.build();

		assertThat(TestUtils.getPropertyValue(interceptor, "retryPolicy.backOff.maxAttempts")).isEqualTo(5L);
	}

	@Test
	public void testWithCustomizedBackOffMoreAttempts() {
		StatefulRetryOperationsInterceptor interceptor =
				RetryInterceptorBuilder.stateful()
						.maxRetries(5)
						.backOffOptions(1, 2, 10)
						.build();

		assertThat(TestUtils.getPropertyValue(interceptor, "retryPolicy.backOff.maxAttempts")).isEqualTo(5L);
		assertThat(TestUtils.getPropertyValue(interceptor, "retryPolicy.backOff.initialInterval")).isEqualTo(1L);
		assertThat(TestUtils.getPropertyValue(interceptor, "retryPolicy.backOff.multiplier")).isEqualTo(2.0);
		assertThat(TestUtils.getPropertyValue(interceptor, "retryPolicy.backOff.maxInterval")).isEqualTo(10L);
	}

	@Test
	public void testWithCustomBackOff() {
		BackOff customBackOff = new FixedBackOff();
		StatefulRetryOperationsInterceptor interceptor =
				RetryInterceptorBuilder.stateful()
						.configureRetryPolicy((retryPolicy) -> retryPolicy.backOff(customBackOff))
						.build();

		assertThat(TestUtils.getPropertyValue(interceptor, "retryPolicy.backOff")).isSameAs(customBackOff);
	}

	@Test
	public void testWithCustomNewMessageIdentifier() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		StatefulRetryOperationsInterceptor interceptor =
				RetryInterceptorBuilder.stateful()
						.configureRetryPolicy((retryPolicy) -> retryPolicy.backOff(new FixedBackOff(10L, 5)))
						.newMessageIdentifier(message -> {
							latch.countDown();
							return false;
						})
						.build();

		assertThat(TestUtils.<FixedBackOff>propertyValue(interceptor, "retryPolicy.backOff"))
				.satisfies((backOff) -> {
					assertThat(backOff.getInterval()).isEqualTo(10L);
					assertThat(backOff.getMaxAttempts()).isEqualTo(5L);
				});
		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder.withBody("".getBytes()).setMessageId("foo").setRedelivered(false).build();

		// First invocation, no need to check if it's a new message
		assertThatRuntimeException()
				.isThrownBy(() -> delegate.onMessage("", message))
				.withMessage("foo");

		assertThat(count.get()).isEqualTo(1);
		assertThat(latch.await(0, TimeUnit.SECONDS)).isFalse();
		// Message has a state
		assertThatRuntimeException()
				.isThrownBy(() -> delegate.onMessage("", message))
				.withMessage("foo");

		assertThat(count).hasValue(2);
		assertThat(latch.await(0, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testWitCustomRetryPolicyTraverseCause() {
		StatefulRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateful()
				.retryPolicy(RetryPolicy.builder().maxRetries(15).build())
				.build();

		assertThat(TestUtils.getPropertyValue(interceptor, "retryPolicy.backOff.maxAttempts")).isEqualTo(15L);
	}

	@Test
	public void testWithCustomKeyGenerator() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		StatefulRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateful()
				.configureRetryPolicy((retryPolicy) -> retryPolicy.delay(Duration.ZERO))
				.messageKeyGenerator(message -> {
					latch.countDown();
					return "foo";
				})
				.build();

		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder.withBody("".getBytes()).setRedelivered(false).build();

		assertThatRuntimeException().isThrownBy(() -> delegate.onMessage("", message))
				.withMessage("foo");

		assertThat(count).hasValue(1);
		assertThat(latch.await(0, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testWithRepublishRecovererExplicitExchangeAndRouting() {
		AmqpTemplate amqpTemplate = mock();

		StatelessRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless()
				.recoverer(new RepublishMessageRecoverer(amqpTemplate, "bar", "baz"))
				.configureRetryPolicy((retryPolicy) -> retryPolicy.delay(Duration.ZERO))
				.build();

		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder.withBody("".getBytes()).build();
		delegate.onMessage("", message);
		assertThat(count.get()).isEqualTo(4);
		verify(amqpTemplate).send("bar", "baz", message);
	}

	@Test
	public void testWithRepublishRecovererDefaultExchangeAndRouting() {
		AmqpTemplate amqpTemplate = mock();

		StatelessRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless()
				.recoverer(new RepublishMessageRecoverer(amqpTemplate) {

					@Override
					protected Map<? extends String, ?> additionalHeaders(Message message, Throwable cause) {
						return Collections.singletonMap("fooHeader", "barValue");
					}

				})
				.configureRetryPolicy((retryPolicy) -> retryPolicy.delay(Duration.ZERO))
				.build();

		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder
				.withBody("".getBytes())
				.setReceivedExchange("exch")
				.setReceivedRoutingKey("foo")
				.build();
		delegate.onMessage("", message);

		assertThat(count).hasValue(4);
		verify(amqpTemplate).send("error.foo", message);

		MessageProperties messageProperties = message.getMessageProperties();
		assertThat(messageProperties.getHeaders())
				.containsKeys(
						RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE,
						RepublishMessageRecoverer.X_EXCEPTION_MESSAGE,
						RepublishMessageRecoverer.X_ORIGINAL_EXCHANGE,
						RepublishMessageRecoverer.X_ORIGINAL_ROUTING_KEY)
				.containsEntry("fooHeader", "barValue");
	}

	@Test
	public void testWithRepublishRecovererDefaultExchangeAndRoutingCustomPrefix() {
		AmqpTemplate amqpTemplate = mock();

		StatelessRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless()
				.recoverer(new RepublishMessageRecoverer(amqpTemplate).errorRoutingKeyPrefix("bar."))
				.configureRetryPolicy((retryPolicy) -> retryPolicy.delay(Duration.ZERO))
				.build();

		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder.withBody("".getBytes()).setReceivedRoutingKey("foo").build();
		delegate.onMessage("", message);
		assertThat(count).hasValue(4);
		verify(amqpTemplate).send("bar.foo", message);
	}

	@Test
	public void testWithRepublishRecovererCustomExchangeAndDefaultRoutingCustomPrefix() {
		AmqpTemplate amqpTemplate = mock();

		StatelessRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless()
				.recoverer(new RepublishMessageRecoverer(amqpTemplate, "baz").errorRoutingKeyPrefix("bar."))
				.configureRetryPolicy((retryPolicy) -> retryPolicy.delay(Duration.ZERO))
				.build();

		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder.withBody("".getBytes()).setReceivedRoutingKey("foo").build();
		delegate.onMessage("", message);
		assertThat(count).hasValue(4);
		verify(amqpTemplate).send("baz", "bar.foo", message);
	}

	@Test
	public void testRequeueRecoverer() {
		StatelessRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless()
				.recoverer(new ImmediateRequeueMessageRecoverer())
				.configureRetryPolicy((retryPolicy) -> retryPolicy.delay(Duration.ZERO))
				.build();

		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder.withBody("".getBytes()).build();
		assertThatExceptionOfType(ImmediateRequeueAmqpException.class)
				.isThrownBy(() -> delegate.onMessage("", message));

		assertThat(count).hasValue(4);
	}

	private Foo createDelegate(MethodInterceptor interceptor, final AtomicInteger count) {
		Foo delegate = (s, message) -> {
			count.incrementAndGet();
			throw new RuntimeException("foo", new RuntimeException("bar"));
		};
		ProxyFactory factory = new ProxyFactory();
		factory.addAdvisor(new DefaultPointcutAdvisor(Pointcut.TRUE, interceptor));
		factory.setProxyTargetClass(false);
		factory.addInterface(Foo.class);
		factory.setTarget(delegate);
		delegate = (Foo) factory.getProxy();
		return delegate;
	}

	interface Foo {

		void onMessage(String s, Message message);

	}

}
