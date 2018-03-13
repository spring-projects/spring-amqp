/*
 * Copyright 2002-2018 the original author or authors.
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.test.MessageTestUtils;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.util.ReflectionUtils;

import com.rabbitmq.client.Channel;

/**
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class RabbitListenerContainerFactoryIntegrationTests {

	private final SimpleRabbitListenerContainerFactory containerFactory = new SimpleRabbitListenerContainerFactory();

	private final DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();

	private final RabbitEndpointSampleBean sample = new RabbitEndpointSampleBean();


	@Before
	public void setup() {
		initializeFactory(factory);
	}

	@Test
	public void messageConverterUsedIfSet() throws Exception {
		containerFactory.setMessageConverter(new UpperCaseMessageConverter());

		MethodRabbitListenerEndpoint endpoint = createDefaultMethodRabbitEndpoint("expectFooBarUpperCase", String.class);
		Message message = MessageTestUtils.createTextMessage("foo-bar");

		invokeListener(endpoint, message);
		assertListenerMethodInvocation("expectFooBarUpperCase");
	}

	private void invokeListener(RabbitListenerEndpoint endpoint, Message message) throws Exception {
		SimpleMessageListenerContainer messageListenerContainer =
				containerFactory.createListenerContainer(endpoint);
		Object listener = messageListenerContainer.getMessageListener();
		if (listener instanceof ChannelAwareMessageListener) {
			((ChannelAwareMessageListener) listener).onMessage(message, mock(Channel.class));
		}
		else {
			((MessageListener) listener).onMessage(message);
		}
	}

	private void assertListenerMethodInvocation(String methodName) {
		assertTrue("Method " + methodName + " should have been invoked", sample.invocations.get(methodName));
	}


	private MethodRabbitListenerEndpoint createMethodRabbitEndpoint(
			DefaultMessageHandlerMethodFactory factory, Method method) {
		MethodRabbitListenerEndpoint endpoint = new MethodRabbitListenerEndpoint();
		endpoint.setBean(sample);
		endpoint.setMethod(method);
		endpoint.setMessageHandlerMethodFactory(factory);
		return endpoint;
	}

	private MethodRabbitListenerEndpoint createDefaultMethodRabbitEndpoint(String methodName, Class<?>... parameterTypes) {
		return createMethodRabbitEndpoint(this.factory, getListenerMethod(methodName, parameterTypes));
	}

	private Method getListenerMethod(String methodName, Class<?>... parameterTypes) {
		Method method = ReflectionUtils.findMethod(RabbitEndpointSampleBean.class, methodName, parameterTypes);
		assertNotNull("no method found with name " + methodName + " and parameters " + Arrays.toString(parameterTypes));
		return method;
	}


	private void initializeFactory(DefaultMessageHandlerMethodFactory factory) {
		factory.setBeanFactory(new StaticListableBeanFactory());
		factory.afterPropertiesSet();
	}


	static class RabbitEndpointSampleBean {

		private final Map<String, Boolean> invocations = new HashMap<String, Boolean>();

		public void expectFooBarUpperCase(@Payload String msg) {
			invocations.put("expectFooBarUpperCase", true);
			assertEquals("Unexpected payload message", "FOO-BAR", msg);
		}
	}


	private static class UpperCaseMessageConverter implements MessageConverter {

		UpperCaseMessageConverter() {
			super();
		}

		@Override
		public Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
			return new Message(object.toString().toUpperCase().getBytes(), new MessageProperties());
		}


		@Override
		public Object fromMessage(Message message) throws MessageConversionException {
			String content = new String(message.getBody());
			return content.toUpperCase();
		}
	}

}
