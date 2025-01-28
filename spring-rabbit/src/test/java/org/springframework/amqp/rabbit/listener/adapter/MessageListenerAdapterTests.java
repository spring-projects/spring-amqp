/*
 * Copyright 2002-2025 the original author or authors.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Dave Syer
 * @author Greg Turnquist
 * @author Gary Russell
 * @author Cai Kun
 * @author Artem Bilan
 *
 */
public class MessageListenerAdapterTests {

	private MessageProperties messageProperties;

	private MessageListenerAdapter adapter;

	private final SimpleService simpleService = new SimpleService();

	@BeforeEach
	public void init() {
		this.messageProperties = new MessageProperties();
		this.messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		this.adapter = new MessageListenerAdapter();
		this.adapter.setMessageConverter(new SimpleMessageConverter());
	}

	@Test
	public void testExtendedListenerAdapter() throws Exception {
		class ExtendedListenerAdapter extends MessageListenerAdapter {

			@Override
			protected Object[] buildListenerArguments(Object extractedMessage, Channel channel, Message message) {
				return new Object[] { extractedMessage, channel, message };
			}

		}
		MessageListenerAdapter extendedAdapter = new ExtendedListenerAdapter();
		final AtomicBoolean called = new AtomicBoolean(false);
		Channel channel = mock(Channel.class);
		class Delegate {

			@SuppressWarnings("unused")
			public void handleMessage(String input, Channel channel, Message message) throws IOException {
				assertThat(input).isNotNull();
				assertThat(channel).isNotNull();
				assertThat(message).isNotNull();
				channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
				called.set(true);
			}

		}
		extendedAdapter.setDelegate(new Delegate());
		extendedAdapter.containerAckMode(AcknowledgeMode.MANUAL);
		extendedAdapter.onMessage(new Message("foo".getBytes(), messageProperties), channel);
		assertThat(called.get()).isTrue();
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
		assertThat(called.get()).isTrue();
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
		assertThat(called.get()).isTrue();
	}

	@Test
	public void testExplicitListenerMethod() throws Exception {
		this.adapter.setDefaultListenerMethod("handle");
		this.adapter.setDelegate(this.simpleService);
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), null);
		assertThat(this.simpleService.called).isEqualTo("handle");
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
		assertThat(this.simpleService.called).isEqualTo("handle");
		this.messageProperties.setConsumerQueue("junk");
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), null);
		assertThat(this.simpleService.called).isEqualTo("notDefinedOnInterface");
		this.messageProperties.setConsumerTag("junk");
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), null);
		assertThat(this.simpleService.called).isEqualTo("anotherHandle");
	}

	@Test
	public void testProxyListener() throws Exception {
		this.adapter.setDefaultListenerMethod("notDefinedOnInterface");
		ProxyFactory factory = new ProxyFactory(this.simpleService);
		factory.setProxyTargetClass(true);
		this.adapter.setDelegate(factory.getProxy());
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), null);
		assertThat(this.simpleService.called).isEqualTo("notDefinedOnInterface");
	}

	@Test
	public void testJdkProxyListener() throws Exception {
		this.adapter.setDefaultListenerMethod("handle");
		ProxyFactory factory = new ProxyFactory(this.simpleService);
		factory.setProxyTargetClass(false);
		this.adapter.setDelegate(factory.getProxy());
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties), null);
		assertThat(this.simpleService.called).isEqualTo("handle");
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
		assertThat(this.simpleService.called).isEqualTo("handle");
		assertThat(replyMessage.get()).isNotNull();
		assertThat(new String(replyMessage.get().getBody())).isEqualTo("processedfoo");
		assertThat(replyAddress.get()).isNotNull();
		assertThat(replyAddress.get().getExchangeName()).isEqualTo("foo");
		assertThat(replyAddress.get().getRoutingKey()).isEqualTo("bar");
		assertThat(throwable.get()).isSameAs(ex);
	}

	@Test
	public void testCompletableFutureReturn() throws Exception {
		class Delegate {

			@SuppressWarnings("unused")
			public CompletableFuture<String> myPojoMessageMethod(String input) {
				CompletableFuture<String> future = new CompletableFuture<>();
				future.complete("processed" + input);
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
