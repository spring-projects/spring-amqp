/*
 * Copyright 2014-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.listener.adapter;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.test.MessageTestUtils;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ReflectionUtils;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

/**
 * @author Stephane Nicoll
 * @author Artem Bilan
 * @author Gary Russell
 */
public class MessagingMessageListenerAdapterTests {

	private final DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();

	private final SampleBean sample = new SampleBean();


	@Before
	public void setup() {
		initializeFactory(factory);
	}

	@Test
	public void buildMessageWithStandardMessage() throws Exception {
		Message<String> result = MessageBuilder.withPayload("Response")
				.setHeader("foo", "bar")
				.setHeader(AmqpHeaders.TYPE, "msg_type")
				.setHeader(AmqpHeaders.REPLY_TO, "reply")
				.build();

		Channel session = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("echo", Message.class);
		org.springframework.amqp.core.Message replyMessage = listener.buildMessage(session, result);

		assertNotNull("reply should never be null", replyMessage);
		assertEquals("Response", new String(replyMessage.getBody()));
		assertEquals("type header not copied", "msg_type", replyMessage.getMessageProperties().getType());
		assertEquals("replyTo header not copied", "reply", replyMessage.getMessageProperties().getReplyTo());
		assertEquals("custom header not copied", "bar", replyMessage.getMessageProperties().getHeaders().get("foo"));
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
			assertEquals(IllegalArgumentException.class, ex.getCause().getClass());
			assertEquals("Expected test exception", ex.getCause().getMessage());
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
			assertEquals(IllegalArgumentException.class, ex.getCause().getClass());
			assertEquals("Expected test exception", ex.getCause().getMessage());
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
			assertEquals(IllegalArgumentException.class, ex.getCause().getClass());
			assertEquals("Expected test exception", ex.getCause().getMessage());
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
			assertThat(ex.getMessage(), containsString("Failed to send reply"));
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
			assertEquals(org.springframework.messaging.converter.MessageConversionException.class,
					ex.getCause().getClass());
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
		assertEquals(String.class, this.sample.payload.getClass());
		message = org.springframework.amqp.core.MessageBuilder
				.withBody("{ \"foo\" : \"bar\" }".getBytes())
				.andProperties(message.getMessageProperties())
				.build();
		listener.onMessage(message, channel);
		assertEquals(LinkedHashMap.class, this.sample.payload.getClass());
	}

	@Test
	public void genericMessageTest2() throws Exception {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("{ \"foo\" : \"bar\" }");
		Channel channel = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("withGenericMessageFooType", Message.class);
		listener.setMessageConverter(new Jackson2JsonMessageConverter());
		message.getMessageProperties().setContentType("application/json");
		listener.onMessage(message, channel);
		assertEquals(Foo.class, this.sample.payload.getClass());
	}


	@Test
	public void genericMessageTest3() throws Exception {
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("{ \"foo\" : \"bar\" }");
		Channel channel = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("withNonGenericMessage", Message.class);
		listener.setMessageConverter(new Jackson2JsonMessageConverter());
		message.getMessageProperties().setContentType("application/json");
		listener.onMessage(message, channel);
		assertEquals(LinkedHashMap.class, this.sample.payload.getClass());
	}

	protected MessagingMessageListenerAdapter getSimpleInstance(String methodName, Class<?>... parameterTypes) {
		Method m = ReflectionUtils.findMethod(SampleBean.class, methodName, parameterTypes);
		return createInstance(m, false);
	}

	protected MessagingMessageListenerAdapter getSimpleInstance(String methodName, boolean returnExceptions,
			Class<?>... parameterTypes) {
		Method m = ReflectionUtils.findMethod(SampleBean.class, methodName, parameterTypes);
		return createInstance(m, returnExceptions);
	}

	protected MessagingMessageListenerAdapter createInstance(Method m, boolean returnExceptions) {
		MessagingMessageListenerAdapter adapter = new MessagingMessageListenerAdapter(null, m, returnExceptions, null);
		adapter.setHandlerMethod(new HandlerAdapter(factory.createInvocableHandlerMethod(sample, m)));
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
		adapter.setHandlerMethod(new HandlerAdapter(handler));
		return adapter;
	}

	private void initializeFactory(DefaultMessageHandlerMethodFactory factory) {
		factory.setBeanFactory(new StaticListableBeanFactory());
		factory.afterPropertiesSet();
	}

	private static class SampleBean {

		private Object payload;

		SampleBean() {
			super();
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
		public String failWithReturn(Integer input) {
			throw new IllegalArgumentException("Expected test exception");
		}

	}

	private static class Foo {

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
