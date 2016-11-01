/*
 * Copyright 2014-2016 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.aopalliance.intercept.MethodInterceptor;
import org.junit.Test;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.aop.Pointcut;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
import org.springframework.retry.interceptor.StatefulRetryOperationsInterceptor;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * @author Gary Russell
 * @since 1.3
 *
 */
public class RetryInterceptorBuilderSupportTests {

	@Test
	public void testBasic() {
		StatefulRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateful().build();
		assertEquals(3, TestUtils.getPropertyValue(interceptor, "retryOperations.retryPolicy.maxAttempts"));
	}

	@Test
	public void testWithCustomRetryTemplate() {
		RetryOperations retryOperations = new RetryTemplate();
		StatefulRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateful()
				.retryOperations(retryOperations)
				.build();
		assertEquals(3, TestUtils.getPropertyValue(interceptor, "retryOperations.retryPolicy.maxAttempts"));
		assertSame(retryOperations, TestUtils.getPropertyValue(interceptor, "retryOperations"));
	}

	@Test
	public void testWithMoreAttempts() {
		StatefulRetryOperationsInterceptor interceptor =
				RetryInterceptorBuilder.stateful()
					.maxAttempts(5)
					.build();
		assertEquals(5, TestUtils.getPropertyValue(interceptor, "retryOperations.retryPolicy.maxAttempts"));
	}

	@Test
	public void testWithCustomizedBackOffMoreAttempts() {
		StatefulRetryOperationsInterceptor interceptor =
				RetryInterceptorBuilder.stateful()
					.maxAttempts(5)
					.backOffOptions(1, 2, 10)
					.build();

		assertEquals(5, TestUtils.getPropertyValue(interceptor, "retryOperations.retryPolicy.maxAttempts"));
		assertEquals(1L, TestUtils.getPropertyValue(interceptor, "retryOperations.backOffPolicy.initialInterval"));
		assertEquals(2.0, TestUtils.getPropertyValue(interceptor, "retryOperations.backOffPolicy.multiplier"));
		assertEquals(10L, TestUtils.getPropertyValue(interceptor, "retryOperations.backOffPolicy.maxInterval"));
	}

	@Test
	public void testWithCustomBackOffPolicy() {
		StatefulRetryOperationsInterceptor interceptor =
				RetryInterceptorBuilder.stateful()
					.maxAttempts(5)
					.backOffPolicy(new FixedBackOffPolicy())
					.build();

		assertEquals(5, TestUtils.getPropertyValue(interceptor, "retryOperations.retryPolicy.maxAttempts"));
		assertEquals(1000L, TestUtils.getPropertyValue(interceptor, "retryOperations.backOffPolicy.backOffPeriod"));
	}

	@Test
	public void testWithCustomNewMessageIdentifier() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		StatefulRetryOperationsInterceptor interceptor =
				RetryInterceptorBuilder.stateful()
					.maxAttempts(5)
					.newMessageIdentifier(message -> {
						latch.countDown();
						return false;
					})
					.backOffPolicy(new FixedBackOffPolicy())
					.build();

		assertEquals(5, TestUtils.getPropertyValue(interceptor, "retryOperations.retryPolicy.maxAttempts"));
		assertEquals(1000L, TestUtils.getPropertyValue(interceptor, "retryOperations.backOffPolicy.backOffPeriod"));
		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder.withBody("".getBytes()).setMessageId("foo").setRedelivered(false).build();
		try {
			delegate.onMessage("", message);
		}
		catch (RuntimeException e) {
			assertEquals("foo", e.getMessage());
		}
		assertEquals(1, count.get());
		assertTrue(latch.await(0, TimeUnit.SECONDS));
	}

	@Test
	public void testWitCustomRetryPolicyTraverseCause() {
		StatefulRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateful()
				.retryPolicy(new SimpleRetryPolicy(15, Collections
						.<Class<? extends Throwable>, Boolean>singletonMap(Exception.class, true), true))
				.build();
		assertEquals(15, TestUtils.getPropertyValue(interceptor, "retryOperations.retryPolicy.maxAttempts"));
	}

	@Test
	public void testWithCustomKeyGenerator() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		StatefulRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateful()
				.messageKeyGenerator(message -> {
					latch.countDown();
					return "foo";
				})
				.build();

		assertEquals(3, TestUtils.getPropertyValue(interceptor, "retryOperations.retryPolicy.maxAttempts"));
		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder.withBody("".getBytes()).setRedelivered(false).build();
		try {
			delegate.onMessage("", message);
		}
		catch (RuntimeException e) {
			assertEquals("foo", e.getMessage());
		}
		assertEquals(1, count.get());
		assertTrue(latch.await(0, TimeUnit.SECONDS));
	}

	@Test
	public void testWithRepublishRecovererExplicitExchangeAndRouting() throws Throwable {
		AmqpTemplate amqpTemplate = mock(AmqpTemplate.class);

		RetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless()
				.recoverer(new RepublishMessageRecoverer(amqpTemplate, "bar", "baz"))
				.build();

		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder.withBody("".getBytes()).build();
		delegate.onMessage("", message);
		assertEquals(3, count.get());
		verify(amqpTemplate).send("bar", "baz", message);
	}

	@Test
	public void testWithRepublishRecovererDefaultExchangeAndRouting() throws Throwable {
		AmqpTemplate amqpTemplate = mock(AmqpTemplate.class);

		RetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless()
				.recoverer(new RepublishMessageRecoverer(amqpTemplate) {

					@Override
					protected Map<? extends String, ? extends Object> additionalHeaders(Message message, Throwable cause) {
						return Collections.singletonMap("fooHeader", "barValue");
					}

				})
				.build();

		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder
				.withBody("".getBytes())
				.setReceivedExchange("exch")
				.setReceivedRoutingKey("foo")
				.build();
		delegate.onMessage("", message);
		assertEquals(3, count.get());
		verify(amqpTemplate).send("error.foo", message);
		assertNotNull(message.getMessageProperties().getHeaders().get(RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE));
		assertNotNull(message.getMessageProperties().getHeaders().get(RepublishMessageRecoverer.X_EXCEPTION_MESSAGE));
		assertNotNull(message.getMessageProperties().getHeaders().get(RepublishMessageRecoverer.X_ORIGINAL_EXCHANGE));
		assertNotNull(message.getMessageProperties().getHeaders().get(RepublishMessageRecoverer.X_ORIGINAL_ROUTING_KEY));
		assertEquals("barValue", message.getMessageProperties().getHeaders().get("fooHeader"));
	}

	@Test
	public void testWithRepublishRecovererDefaultExchangeAndRoutingCustomPrefix() throws Throwable {
		AmqpTemplate amqpTemplate = mock(AmqpTemplate.class);

		RetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless()
				.recoverer(new RepublishMessageRecoverer(amqpTemplate).errorRoutingKeyPrefix("bar."))
				.build();

		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder.withBody("".getBytes()).setReceivedRoutingKey("foo").build();
		delegate.onMessage("", message);
		assertEquals(3, count.get());
		verify(amqpTemplate).send("bar.foo", message);
	}

	@Test
	public void testWithRepublishRecovererCustomExchangeAndDefaultRoutingCustomPrefix() throws Throwable {
		AmqpTemplate amqpTemplate = mock(AmqpTemplate.class);

		RetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless()
				.recoverer(new RepublishMessageRecoverer(amqpTemplate, "baz").errorRoutingKeyPrefix("bar."))
				.build();

		final AtomicInteger count = new AtomicInteger();
		Foo delegate = createDelegate(interceptor, count);
		Message message = MessageBuilder.withBody("".getBytes()).setReceivedRoutingKey("foo").build();
		delegate.onMessage("", message);
		assertEquals(3, count.get());
		verify(amqpTemplate).send("baz", "bar.foo", message);
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
