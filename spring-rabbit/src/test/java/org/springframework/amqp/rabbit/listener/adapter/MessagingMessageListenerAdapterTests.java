/*
 * Copyright 2014-2025 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.test.MessageTestUtils;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Stephane Nicoll
 * @author Artem Bilan
 * @author Gary Russell
 * @author Kai Stapel
 */
public class MessagingMessageListenerAdapterTests {

	private final DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();

	private final SampleBean sample = new SampleBean();


	@BeforeEach
	public void setup() {
		initializeFactory(factory);
	}

	@Test
	public void buildMessageWithStandardMessage() {
		Message<String> result = MessageBuilder.withPayload("Response")
				.setHeader("foo", "bar")
				.setHeader(AmqpHeaders.TYPE, "msg_type")
				.setHeader(AmqpHeaders.REPLY_TO, "reply")
				.build();

		Channel session = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("echo", Message.class);
		org.springframework.amqp.core.Message replyMessage = listener.buildMessage(session, result, null);

		assertThat(replyMessage).as("reply should never be null").isNotNull();
		assertThat(new String(replyMessage.getBody())).isEqualTo("Response");
		assertThat(replyMessage.getMessageProperties().getType()).as("type header not copied").isEqualTo("msg_type");
		assertThat(replyMessage.getMessageProperties().getReplyTo()).as("replyTo header not copied").isEqualTo("reply");
		assertThat(replyMessage.getMessageProperties().getHeaders().get("foo")).as("custom header not copied").isEqualTo("bar");
	}

	@Test
	public void exceptionInListener() {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("foo");
		Channel channel = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("fail", String.class);

		try {
			listener.onMessage(message, channel);
			fail("Should have thrown an exception");
		}
		catch (ListenerExecutionFailedException ex) {
			assertThat(ex.getCause().getClass()).isEqualTo(IllegalArgumentException.class);
			assertThat(ex.getCause().getMessage()).isEqualTo("Expected test exception");
		}
		catch (Exception ex) {
			fail("Should not have thrown an " + ex.getClass().getSimpleName());
		}
	}

	@Test
	public void exceptionInListenerBadReturnExceptionSetting() {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("foo");
		Channel channel = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("fail", true, String.class);
		try {
			listener.onMessage(message, channel);
			fail("Should have thrown an exception");
		}
		catch (ListenerExecutionFailedException ex) {
			assertThat(ex.getCause().getClass()).isEqualTo(IllegalArgumentException.class);
			assertThat(ex.getCause().getMessage()).isEqualTo("Expected test exception");
		}
		catch (Exception ex) {
			fail("Should not have thrown an " + ex.getClass().getSimpleName());
		}
	}

	@Test
	public void exceptionInMultiListenerReturnException() throws Exception {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("foo");
		Channel channel = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getMultiInstance("fail", "failWithReturn", true, String.class,
				Integer.class);
		try {
			listener.onMessage(message, channel);
			fail("Should have thrown an exception");
		}
		catch (ListenerExecutionFailedException ex) {
			assertThat(ex.getCause().getClass()).isEqualTo(IllegalArgumentException.class);
			assertThat(ex.getCause().getMessage()).isEqualTo("Expected test exception");
		}
		catch (Exception ex) {
			ex.printStackTrace();
			fail("Should not have thrown an " + ex.getClass().getSimpleName());
		}
		message = new SimpleMessageConverter().toMessage(42, new MessageProperties());
		try {
			listener.onMessage(message, channel);
			fail("Should have thrown an exception");
		}
		catch (ReplyFailureException ex) {
			assertThat(ex.getMessage()).contains("Failed to send reply");
		}
		catch (Exception ex) {
			fail("Should not have thrown an " + ex.getClass().getSimpleName());
		}
		message.getMessageProperties().setReplyTo("foo/bar");
		listener.onMessage(message, channel);
		verify(channel).basicPublish(eq("foo"), eq("bar"), eq(false), any(BasicProperties.class), any(byte[].class));
	}

	@Test
	public void exceptionInInvocation() {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("foo");
		Channel channel = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("wrongParam", Integer.class);

		try {
			listener.onMessage(message, channel);
			fail("Should have thrown an exception");
		}
		catch (ListenerExecutionFailedException ex) {
			assertThat(ex.getCause().getClass()).isEqualTo(org.springframework.messaging.converter.MessageConversionException.class);
		}
		catch (Exception ex) {
			fail("Should not have thrown an " + ex.getClass().getSimpleName());
		}
	}

	@Test
	public void genericMessageTest1() throws Exception {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("\"foo\"");
		Channel channel = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("withGenericMessageAnyType", Message.class);
		listener.setMessageConverter(new Jackson2JsonMessageConverter());
		message.getMessageProperties().setContentType("application/json");
		listener.onMessage(message, channel);
		assertThat(this.sample.payload.getClass()).isEqualTo(String.class);
		message = org.springframework.amqp.core.MessageBuilder
				.withBody("{ \"foo\" : \"bar\" }".getBytes())
				.andProperties(message.getMessageProperties())
				.build();
		listener.onMessage(message, channel);
		assertThat(this.sample.payload.getClass()).isEqualTo(LinkedHashMap.class);
	}

	@Test
	void headers() throws Exception {
		MessagingMessageListenerAdapter listener = getSimpleInstance("withHeaders", Foo.class, Map.class);
		assertThat(TestUtils.getPropertyValue(listener, "messagingMessageConverter.inferredArgumentType"))
				.isEqualTo(Foo.class);
	}

	@Test
	public void genericMessageTest2() throws Exception {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("{ \"foo\" : \"bar\" }");
		Channel channel = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("withGenericMessageFooType", Message.class);
		listener.setMessageConverter(new Jackson2JsonMessageConverter());
		message.getMessageProperties().setContentType("application/json");
		listener.onMessage(message, channel);
		assertThat(this.sample.payload.getClass()).isEqualTo(Foo.class);
	}


	@Test
	public void genericMessageTest3() throws Exception {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("{ \"foo\" : \"bar\" }");
		Channel channel = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("withNonGenericMessage", Message.class);
		listener.setMessageConverter(new Jackson2JsonMessageConverter());
		message.getMessageProperties().setContentType("application/json");
		listener.onMessage(message, channel);
		assertThat(this.sample.payload.getClass()).isEqualTo(LinkedHashMap.class);
	}

	@Test
	public void batchAmqpMessagesTest() {
		// given
		org.springframework.amqp.core.Message message1 = MessageTestUtils.createTextMessage("{ \"foo1\" : \"bar1\" }");
		message1.getMessageProperties().setContentType("application/json");
		Channel channel = mock(Channel.class);
		BatchMessagingMessageListenerAdapter listener = getBatchInstance("withAmqpMessageBatch");
		listener.setMessageConverter(new Jackson2JsonMessageConverter());

		// when
		listener.onMessageBatch(Arrays.asList(message1), channel);

		// then
		assertThat(this.sample.batchPayloads.get(0).getClass()).isEqualTo(String.class);
	}

	@Test
	public void batchTypedMessagesTest() {
		// given
		org.springframework.amqp.core.Message message1 = MessageTestUtils.createTextMessage("{ \"foo1\" : \"bar1\" }");
		message1.getMessageProperties().setContentType("application/json");
		Channel channel = mock(Channel.class);
		BatchMessagingMessageListenerAdapter listener = getBatchInstance("withTypedMessageBatch");
		listener.setMessageConverter(new Jackson2JsonMessageConverter());

		// when
		listener.onMessageBatch(Arrays.asList(message1), channel);

		// then
		assertThat(this.sample.batchPayloads.get(0).getClass()).isEqualTo(Foo.class);
	}

	@Test
	public void batchTypedObjectTest() {
		// given
		org.springframework.amqp.core.Message message1 = MessageTestUtils.createTextMessage("{ \"foo1\" : \"bar1\" }");
		message1.getMessageProperties().setContentType("application/json");
		Channel channel = mock(Channel.class);
		BatchMessagingMessageListenerAdapter listener = getBatchInstance("withFooBatch");
		listener.setMessageConverter(new Jackson2JsonMessageConverter());

		// when
		listener.onMessageBatch(Arrays.asList(message1), channel);

		// then
		assertThat(this.sample.batchPayloads.get(0).getClass()).isEqualTo(Foo.class);
	}

	@Test
	void errorHandlerAfterConversionEx() throws Exception {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("foo");
		Channel channel = mock(Channel.class);
		AtomicBoolean ehCalled = new AtomicBoolean();
		MessagingMessageListenerAdapter listener = getSimpleInstance("fail",
				(amqpMessage, channel1, message1, exception) -> {
					ehCalled.set(true);
					return null;
				}, false, String.class);
		listener.setMessageConverter(new MessageConverter() {

			@Override
			public org.springframework.amqp.core.Message toMessage(Object object, MessageProperties messageProperties)
					throws MessageConversionException {

				return null;
			}

			@Override
			public Object fromMessage(org.springframework.amqp.core.Message message) throws MessageConversionException {
				throw new MessageConversionException("test");
			}
		});
		listener.onMessage(message, channel);
		assertThat(ehCalled.get()).isTrue();
	}

	@Test
	void errorHandlerAfterConversionExWithResult() throws Exception {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("foo");
		Channel channel = mock(Channel.class);
		AtomicBoolean ehCalled = new AtomicBoolean();
		MessagingMessageListenerAdapter listener = getSimpleInstance("fail",
				(amqpMessage, channel1, message1, exception) -> {
					ehCalled.set(true);
					return "foo";
				}, false, String.class);
		listener.setMessageConverter(new MessageConverter() {

			@Override
			public org.springframework.amqp.core.Message toMessage(Object object, MessageProperties messageProperties)
					throws MessageConversionException {

				return new org.springframework.amqp.core.Message(((String) object).getBytes(), messageProperties);
			}

			@Override
			public Object fromMessage(org.springframework.amqp.core.Message message) throws MessageConversionException {
				throw new MessageConversionException("test");
			}
		});
		listener.setResponseAddress("foo/bar");
		listener.onMessage(message, channel);
		verify(channel).basicPublish(any(), any(), anyBoolean(), any(), any());
		assertThat(ehCalled.get()).isTrue();
	}

	@Test
	void errorHandlerAfterConversionExWithReturnEx() throws Exception {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("foo");
		Channel channel = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("fail", null, true, String.class);
		class MC extends SimpleMessageConverter {

			@Override
			public Object fromMessage(org.springframework.amqp.core.Message message) throws MessageConversionException {
				throw new MessageConversionException("test");
			}

		}
		listener.setMessageConverter(new MC());
		listener.setResponseAddress("foo/bar");
		listener.onMessage(message, channel);
		verify(channel).basicPublish(any(), any(), anyBoolean(), any(), any());
	}

	protected MessagingMessageListenerAdapter getSimpleInstance(String methodName, Class<?>... parameterTypes) {
		return getSimpleInstance(methodName, null, false, parameterTypes);
	}

	protected MessagingMessageListenerAdapter getSimpleInstance(String methodName, RabbitListenerErrorHandler eh,
			boolean returnEx, Class<?>... parameterTypes) {

		Method m = ReflectionUtils.findMethod(SampleBean.class, methodName, parameterTypes);
		return createInstance(m, returnEx, eh);
	}

	protected MessagingMessageListenerAdapter getSimpleInstance(String methodName, boolean returnExceptions,
			Class<?>... parameterTypes) {
		Method m = ReflectionUtils.findMethod(SampleBean.class, methodName, parameterTypes);
		return createInstance(m, returnExceptions);
	}

	protected MessagingMessageListenerAdapter createInstance(Method m, boolean returnExceptions) {
		return createInstance(m, returnExceptions, null);
	}

	protected MessagingMessageListenerAdapter createInstance(Method m, boolean returnExceptions,
			RabbitListenerErrorHandler eh) {

		MessagingMessageListenerAdapter adapter = new MessagingMessageListenerAdapter(null, m, returnExceptions, eh);
		adapter.setHandlerAdapter(new HandlerAdapter(factory.createInvocableHandlerMethod(sample, m)));
		return adapter;
	}

	protected MessagingMessageListenerAdapter getMultiInstance(String methodName1, String methodName2,
			boolean returnExceptions, Class<?> m1ParameterType, Class<?> m2ParameterType) {
		Method m1 = ReflectionUtils.findMethod(SampleBean.class, methodName1, m1ParameterType);
		Method m2 = ReflectionUtils.findMethod(SampleBean.class, methodName2, m2ParameterType);
		return createMultiInstance(m1, m2, returnExceptions);
	}

	protected MessagingMessageListenerAdapter createMultiInstance(Method m1, Method m2, boolean returnExceptions) {
		MessagingMessageListenerAdapter adapter = new MessagingMessageListenerAdapter(null, null, returnExceptions, null);
		List<InvocableHandlerMethod> methods = new ArrayList<>();
		methods.add(this.factory.createInvocableHandlerMethod(sample, m1));
		methods.add(this.factory.createInvocableHandlerMethod(sample, m2));
		DelegatingInvocableHandler handler = new DelegatingInvocableHandler(methods, this.sample, null, null);
		adapter.setHandlerAdapter(new HandlerAdapter(handler));
		return adapter;
	}

	protected BatchMessagingMessageListenerAdapter getBatchInstance(String methodName) {
		Method m = ReflectionUtils.findMethod(SampleBean.class, methodName, List.class);
		return createBatchInstance(m);
	}

	protected BatchMessagingMessageListenerAdapter createBatchInstance(Method m) {
		BatchMessagingMessageListenerAdapter adapter = new BatchMessagingMessageListenerAdapter(null, m, false, null, null);
		adapter.setHandlerAdapter(new HandlerAdapter(factory.createInvocableHandlerMethod(sample, m)));
		return adapter;
	}

	private void initializeFactory(DefaultMessageHandlerMethodFactory factory) {
		factory.setBeanFactory(new StaticListableBeanFactory());
		factory.afterPropertiesSet();
	}

	private static class SampleBean {

		private Object payload;
		private List<Object> batchPayloads;

		SampleBean() {
		}

		@SuppressWarnings("unused")
		public Message<String> echo(Message<String> input) {
			return MessageBuilder.withPayload(input.getPayload())
					.setHeader(AmqpHeaders.TYPE, "reply")
					.build();
		}

		@SuppressWarnings("unused")
		public void fail(String input) {
			throw new IllegalArgumentException("Expected test exception");
		}

		@SuppressWarnings("unused")
		public void wrongParam(Integer i) {
			throw new IllegalArgumentException("Should not have been called");
		}

		@SuppressWarnings("unused")
		public void withGenericMessageAnyType(Message<?> message) {
			this.payload = message.getPayload();
		}

		@SuppressWarnings("unused")
		public void withFoo(Foo foo) {
			this.payload = foo;
		}

		@SuppressWarnings("unused")
		public void withGenericMessageFooType(Message<Foo> message) {
			this.payload = message.getPayload();
		}

		@SuppressWarnings("unused")
		public void withNonGenericMessage(@SuppressWarnings("rawtypes") Message message) {
			this.payload = message.getPayload();
		}

		@SuppressWarnings("unused")
		public void withAmqpMessageBatch(List<org.springframework.amqp.core.Message> messageBatch) {
			this.batchPayloads = messageBatch.stream().map(m -> new String(m.getBody())).collect(Collectors.toList());
		}

		@SuppressWarnings("unused")
		public void withTypedMessageBatch(List<Message<Foo>> messageBatch) {
			this.batchPayloads = messageBatch.stream().map(Message::getPayload).collect(Collectors.toList());
		}

		@SuppressWarnings("unused")
		public void withFooBatch(List<Foo> messageBatch) {
			this.batchPayloads = new ArrayList<>(messageBatch);
		}

		@SuppressWarnings("unused")
		public String failWithReturn(Integer input) {
			throw new IllegalArgumentException("Expected test exception");
		}

		@SuppressWarnings("unused")
		public void withHeaders(Foo foo, @Headers Map<String, Object> headers) {
		}

	}

	private static final class Foo {

		private String foo;

		@SuppressWarnings("unused")
		public String getFoo() {
			return this.foo;
		}

		@SuppressWarnings("unused")
		public void setFoo(String foo) {
			this.foo = foo;
		}

	}

}
