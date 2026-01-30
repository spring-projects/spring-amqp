/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.listener.adapter.HandlerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 2.2
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class MessageHandlerTests {

	@Autowired
	private RabbitListenerAnnotationBeanPostProcessor bpp;

	@Test
	void testMessages() throws Exception {
		MessageHandlerMethodFactory factory = this.bpp.getMessageHandlerMethodFactory();
		Foo foo = new Foo();
		Map<String, Method> methods = new HashMap<>();
		ReflectionUtils.doWithMethods(Foo.class,
				method -> methods.put(method.getName(), method),
				method -> method.getName().equals("listen1"));
		ReflectionUtils.doWithMethods(Foo.class,
				method -> methods.put(method.getName(), method),
				method -> method.getName().equals("listen2"));
		InvocableHandlerMethod invMethod = factory.createInvocableHandlerMethod(foo, methods.get("listen1"));
		List<GenericMessage<String>> messagingMessages = Collections.singletonList(new GenericMessage<>("foo"));
		List<org.springframework.amqp.core.Message> amqpMessages = Collections.singletonList(
				new org.springframework.amqp.core.Message("bar".getBytes(), new MessageProperties()));
		HandlerAdapter adapter = new HandlerAdapter(invMethod);
		adapter.invoke(new GenericMessage<>(messagingMessages), null, mock(Channel.class));
		assertThat(foo.messagingMessages).isSameAs(messagingMessages);
		invMethod = factory.createInvocableHandlerMethod(foo, methods.get("listen2"));
		adapter = new HandlerAdapter(invMethod);
		adapter.invoke(new GenericMessage<>(amqpMessages), null, mock(Channel.class));
		assertThat(foo.rabbitMessages).isSameAs(amqpMessages);
	}

	@Configuration
	@EnableRabbit
	public static class Config {


	}

	public static class Foo {

		List<Message<?>> messagingMessages;

		List<org.springframework.amqp.core.Message> rabbitMessages;

		public void listen1(List<Message<?>> mMessages) {
			this.messagingMessages = mMessages;
		}

		public void listen2(List<org.springframework.amqp.core.Message> amqpMessages) {
			this.rabbitMessages = amqpMessages;
		}

	}

}
