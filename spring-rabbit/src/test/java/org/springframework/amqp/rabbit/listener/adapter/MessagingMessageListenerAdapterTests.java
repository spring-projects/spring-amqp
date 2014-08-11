/*
 * Copyright 2002-2014 the original author or authors.
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

import java.lang.reflect.Method;

import com.rabbitmq.client.Channel;
import org.junit.Before;
import org.junit.Test;

import org.springframework.amqp.rabbit.listener.ListenerExecutionFailedException;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ReflectionUtils;

import static org.junit.Assert.*;
import static org.mockito.BDDMockito.*;
import static org.springframework.amqp.rabbit.test.MessageTestUtils.*;

/**
 * @author Stephane Nicoll
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
		org.springframework.amqp.core.Message message = createTextMessage("foo");
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
			fail("Should not have thrown another exception");
		}
	}

	@Test
	public void exceptionInInvocation() {
		org.springframework.amqp.core.Message message = createTextMessage("foo");
		Channel channel = mock(Channel.class);
		MessagingMessageListenerAdapter listener = getSimpleInstance("wrongParam", Integer.class);

		try {
			listener.onMessage(message, channel);
			fail("Should have thrown an exception");
		}
		catch (ListenerExecutionFailedException ex) {
			assertEquals(MessageConversionException.class, ex.getCause().getClass());
		}
		catch (Exception ex) {
			fail("Should not have thrown another exception");
		}
	}

	protected MessagingMessageListenerAdapter getSimpleInstance(String methodName, Class... parameterTypes) {
		Method m = ReflectionUtils.findMethod(SampleBean.class, methodName, parameterTypes);
		return createInstance(m);
	}

	protected MessagingMessageListenerAdapter createInstance(Method m) {
		MessagingMessageListenerAdapter adapter = new MessagingMessageListenerAdapter();
		adapter.setHandlerMethod(factory.createInvocableHandlerMethod(sample, m));
		return adapter;
	}

	private void initializeFactory(DefaultMessageHandlerMethodFactory factory) {
		factory.setBeanFactory(new StaticListableBeanFactory());
		factory.afterPropertiesSet();
	}

	private static class SampleBean {

		public Message<String> echo(Message<String> input) {
			return MessageBuilder.withPayload(input.getPayload())
					.setHeader(AmqpHeaders.TYPE, "reply")
					.build();
		}

		public void fail(String input) {
			throw new IllegalArgumentException("Expected test exception");
		}

		public void wrongParam(Integer i) {
			throw new IllegalArgumentException("Should not have been called");
		}
	}

}
