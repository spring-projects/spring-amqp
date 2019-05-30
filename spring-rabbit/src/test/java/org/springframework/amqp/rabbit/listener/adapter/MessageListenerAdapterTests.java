/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.listener.adapter;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.SendRetryContextAccessor;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.rabbitmq.client.Channel;
import reactor.core.publisher.Mono;

/**
 * @author Dave Syer
 * @author Greg Turnquist
 * @author Gary Russell
 * @author Artem Bilan
 */
public class MessageListenerAdapterTests {

	private MessageProperties messageProperties;

	private MessageListenerAdapter adapter;

	private final SimpleService simpleService = new SimpleService();

	@Before
	public void init() {
		this.messageProperties = new MessageProperties();
		this.messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		this.adapter = new MessageListenerAdapter();
		this.adapter.setMessageConverter(new SimpleMessageConverter());
	}

	@Test
	public void testDefaultListenerMethod() throws Exception {
		final AtomicBoolean called = new AtomicBoolean(false);
		class Delegate {
			@SuppressWarnings("unused")
			public String handleMessage(String input) {
				called.set(true);
				return "processed" + input;
			}
		}
		this.adapter.setDelegate(new Delegate());
		this.adapter.onMessage(new Message("foo".getBytes(), messageProperties), null);
		assertTrue(called.get());
	}

	@Test
	public void testAlternateConstructor() throws Exception {
		final AtomicBoolean called = new AtomicBoolean(false);
		class Delegate {
			@SuppressWarnings("unused")
			public String myPojoMessageMethod(String input) {
				called.set(true);
				return "processed" + input;
			}
		}
		this.adapter = new MessageListenerAdapter(new Delegate(), "myPojoMessageMethod");
		this.adapter.onMessage(new Message("foo".getBytes(), messageProperties), null);
		assertTrue(called.get());
	}

	@Test
	public void testExplicitListenerMethod() throws Exception {
		this.adapter.setDefaultListenerMethod("handle");
		this.adapter.setDelegate(this.simpleService);
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), null);
		assertEquals("handle", this.simpleService.called);
	}

	@Test
	public void testMappedListenerMethod() throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		map.put("foo", "handle");
		map.put("bar", "notDefinedOnInterface");
		this.adapter.setDefaultListenerMethod("anotherHandle");
		this.adapter.setQueueOrTagToMethodName(map);
		this.adapter.setDelegate(this.simpleService);
		this.messageProperties.setConsumerQueue("foo");
		this.messageProperties.setConsumerTag("bar");
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), null);
		assertEquals("handle", this.simpleService.called);
		this.messageProperties.setConsumerQueue("junk");
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), null);
		assertEquals("notDefinedOnInterface", this.simpleService.called);
		this.messageProperties.setConsumerTag("junk");
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), null);
		assertEquals("anotherHandle", this.simpleService.called);
	}

	@Test
	public void testProxyListener() throws Exception {
		this.adapter.setDefaultListenerMethod("notDefinedOnInterface");
		ProxyFactory factory = new ProxyFactory(this.simpleService);
		factory.setProxyTargetClass(true);
		this.adapter.setDelegate(factory.getProxy());
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), null);
		assertEquals("notDefinedOnInterface", this.simpleService.called);
	}

	@Test
	public void testJdkProxyListener() throws Exception {
		this.adapter.setDefaultListenerMethod("handle");
		ProxyFactory factory = new ProxyFactory(this.simpleService);
		factory.setProxyTargetClass(false);
		this.adapter.setDelegate(factory.getProxy());
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), null);
		assertEquals("handle", this.simpleService.called);
	}

	@Test
	public void testReplyRetry() throws Exception {
		this.adapter.setDefaultListenerMethod("handle");
		this.adapter.setDelegate(this.simpleService);
		RetryPolicy retryPolicy = new SimpleRetryPolicy(2);
		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(retryPolicy);
		this.adapter.setRetryTemplate(retryTemplate);
		AtomicReference<Message> replyMessage = new AtomicReference<>();
		AtomicReference<Address> replyAddress = new AtomicReference<>();
		AtomicReference<Throwable> throwable = new AtomicReference<>();
		this.adapter.setRecoveryCallback(ctx -> {
			replyMessage.set(SendRetryContextAccessor.getMessage(ctx));
			replyAddress.set(SendRetryContextAccessor.getAddress(ctx));
			throwable.set(ctx.getLastThrowable());
			return null;
		});
		this.messageProperties.setReplyTo("foo/bar");
		Channel channel = mock(Channel.class);
		RuntimeException ex = new RuntimeException();
		willThrow(ex).given(channel)
			.basicPublish(eq("foo"), eq("bar"), eq(Boolean.FALSE), any(), any());
		Message message = new Message("foo".getBytes(), this.messageProperties);
		this.adapter.onMessage(message, channel);
		assertThat(this.simpleService.called, equalTo("handle"));
		assertThat(replyMessage.get(), notNullValue());
		assertThat(new String(replyMessage.get().getBody()), equalTo("processedfoo"));
		assertThat(replyAddress.get(), notNullValue());
		assertThat(replyAddress.get().getExchangeName(), equalTo("foo"));
		assertThat(replyAddress.get().getRoutingKey(), equalTo("bar"));
		assertThat(throwable.get(), sameInstance(ex));
	}

	@Test
	public void testListenableFutureReturn() throws Exception {
		class Delegate {

			@SuppressWarnings("unused")
			public ListenableFuture<String> myPojoMessageMethod(String input) {
				SettableListenableFuture<String> future = new SettableListenableFuture<>();
				future.set("processed" + input);
				return future;
			}

		}
		this.adapter = new MessageListenerAdapter(new Delegate(), "myPojoMessageMethod");
		this.adapter.containerAckMode(AcknowledgeMode.MANUAL);
		this.adapter.setResponseExchange("default");
		Channel mockChannel = mock(Channel.class);
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), mockChannel);
		verify(mockChannel).basicAck(anyLong(), eq(false));
	}

	@Test
	public void testMonoVoidReturnAck() throws Exception {
		class Delegate {

			@SuppressWarnings("unused")
			public Mono<Void> myPojoMessageMethod(String input) {
				return Mono.empty();
			}

		}
		this.adapter = new MessageListenerAdapter(new Delegate(), "myPojoMessageMethod");
		this.adapter.containerAckMode(AcknowledgeMode.MANUAL);
		this.adapter.setResponseExchange("default");
		Channel mockChannel = mock(Channel.class);
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), mockChannel);
		verify(mockChannel).basicAck(anyLong(), eq(false));
	}

	public interface Service {

		String handle(String input);

		String anotherHandle(String input);

	}

	public static class SimpleService implements Service {

		private String called;

		@Override
		public String handle(String input) {
			called = "handle";
			return "processed" + input;
		}

		@Override
		public String anotherHandle(String input) {
			called = "anotherHandle";
			return "processed" + input;
		}

		public String notDefinedOnInterface(String input) {
			called = "notDefinedOnInterface";
			return "processed" + input;
		}

	}
}
