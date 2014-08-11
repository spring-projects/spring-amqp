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

package org.springframework.amqp.rabbit.config;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.ReplyFailureException;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.AmqpMessageHeaderAccessor;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MethodArgumentTypeMismatchException;
import org.springframework.util.ReflectionUtils;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.validation.annotation.Validated;

import static org.junit.Assert.*;
import static org.mockito.AdditionalMatchers.*;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.*;
import static org.springframework.amqp.rabbit.test.MessageTestUtils.*;


/**
 * @author Stephane Nicoll
 */
public class MethodRabbitListenerEndpointTests {

	@Rule
	public final TestName name = new TestName();

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	private final DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();

	private final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();

	private final RabbitEndpointSampleBean sample = new RabbitEndpointSampleBean();


	@Before
	public void setup() {
		initializeFactory(factory);
	}

	@Test
	public void createMessageListenerNoFactory() {
		MethodRabbitListenerEndpoint endpoint = new MethodRabbitListenerEndpoint();
		endpoint.setBean(this);
		endpoint.setMethod(getTestMethod());

		thrown.expect(IllegalStateException.class);
		endpoint.createMessageListener(container);
	}

	@Test
	public void createMessageListener() {
		MethodRabbitListenerEndpoint endpoint = new MethodRabbitListenerEndpoint();
		endpoint.setBean(this);
		endpoint.setMethod(getTestMethod());
		endpoint.setMessageHandlerMethodFactory(factory);

		assertNotNull(endpoint.createMessageListener(container));
	}

	@Test
	public void resolveMessageAndSession() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(
				org.springframework.amqp.core.Message.class, Channel.class);

		Channel channel = mock(Channel.class);
		listener.onMessage(createTextMessage("test"), channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void resolveGenericMessage() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(Message.class);

		Channel channel = mock(Channel.class);
		listener.onMessage(createTextMessage("test"), channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void resolveHeaderAndPayload() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(String.class, int.class);

		Channel channel = mock(Channel.class);
		MessageProperties properties = new MessageProperties();
		properties.setHeader("myCounter", 55);
		org.springframework.amqp.core.Message message = createTextMessage("my payload", properties);
		listener.onMessage(message, channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void resolveCustomHeaderNameAndPayload() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(String.class, int.class);

		Channel channel = mock(Channel.class);
		MessageProperties properties = new MessageProperties();
		properties.setHeader("myCounter", 24);
		org.springframework.amqp.core.Message message = createTextMessage("my payload", properties);
		listener.onMessage(message, channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void resolveHeaders() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(String.class, Map.class);

		Channel channel = mock(Channel.class);
		MessageProperties properties = new MessageProperties();
		properties.setHeader("customInt", 1234);
		properties.setMessageId("abcd-1234");
		org.springframework.amqp.core.Message message = createTextMessage("my payload", properties);
		listener.onMessage(message, channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void resolveMessageHeaders() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(MessageHeaders.class);

		Channel channel = mock(Channel.class);
		MessageProperties properties = new MessageProperties();
		properties.setHeader("customLong", 4567L);
		properties.setType("myMessageType");
		org.springframework.amqp.core.Message message = createTextMessage("my payload", properties);
		listener.onMessage(message, channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void resolveRabbitMessageHeaderAccessor() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(AmqpMessageHeaderAccessor.class);

		Channel channel = mock(Channel.class);
		MessageProperties properties = new MessageProperties();
		properties.setHeader("customBoolean", true);
		properties.setAppId("myAppId");
		org.springframework.amqp.core.Message message = createTextMessage("my payload", properties);
		listener.onMessage(message, channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void resolveObjectPayload() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(MyBean.class);
		MyBean myBean = new MyBean();
		myBean.name = "myBean name";

		Channel channel = mock(Channel.class);
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
		org.springframework.amqp.core.Message message =
				new org.springframework.amqp.core.Message(SerializationUtils.serialize(myBean), messageProperties);
		listener.onMessage(message, channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void resolveConvertedPayload() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(Integer.class);

		Channel channel = mock(Channel.class);

		listener.onMessage(createTextMessage("33"), channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void processAndReply() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(String.class);
		String body = "echo text";
		String correlationId = "link-1234";
		String responseExchange = "fooQueue";
		String responseRoutingKey = "abc-1234";

		listener.setResponseExchange(responseExchange);
		listener.setResponseRoutingKey(responseRoutingKey);
		MessageProperties properties = new MessageProperties();
		properties.setCorrelationId(correlationId.getBytes(SimpleMessageConverter.DEFAULT_CHARSET));
		org.springframework.amqp.core.Message message = createTextMessage(body, properties);

		processAndReply(listener, message, responseExchange, responseRoutingKey, false, correlationId);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void processAndReplyWithMessage() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(org.springframework.amqp.core.Message.class);
		listener.setMessageConverter(null);
		listener.setResponseExchange("fooQueue");
		String body = "echo text";

		org.springframework.amqp.core.Message message = createTextMessage(body, new MessageProperties());


		processAndReply(listener, message, "fooQueue", "", false, null);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void processAndReplyWithMessageAndStringReply() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(org.springframework.amqp.core.Message.class);
		listener.setMessageConverter(null);
		listener.setResponseExchange("fooQueue");
		String body = "echo text";

		org.springframework.amqp.core.Message message = createTextMessage(body, new MessageProperties());

		try {
			processAndReply(listener, message, "fooQueue", "", false, null);
			fail("Should have fail. Not converter and the reply is not a message");
		}
		catch (ReplyFailureException ex) {
			Throwable cause = ex.getCause();
			assertNotNull(cause);
			assertEquals(MessageConversionException.class, cause.getClass());
			assertTrue(ex.getMessage().contains("foo")); // exception holds the content of the reply
		}
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void processAndReplyUsingReplyTo() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(String.class);
		listener.setMandatoryPublish(true);
		String body = "echo text";
		Address replyTo = new Address(null, "replyToQueue", "myRouting");

		MessageProperties properties = new MessageProperties();
		properties.setReplyToAddress(replyTo);
		org.springframework.amqp.core.Message message = createTextMessage(body, properties);


		processAndReply(listener, message, "replyToQueue", "myRouting", true, null);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void processAndReplyWithSendTo() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(String.class);
		String body = "echo text";
		String messageId = "msgId-1234";

		MessageProperties properties = new MessageProperties();
		properties.setMessageId(messageId);
		org.springframework.amqp.core.Message message = createTextMessage(body, properties);

		// MessageId is used as fallback when no correlationId is set
		processAndReply(listener, message, "replyDestination", "", false, messageId);
		assertDefaultListenerMethodInvocation();
	}

	public void processAndReply(MessagingMessageListenerAdapter listener,
			org.springframework.amqp.core.Message message, String expectedExchange, String routingKey,
			boolean mandatory, String expectedCorrelationId) throws Exception {

		Channel channel = mock(Channel.class);

		listener.onMessage(message, channel);

		ArgumentCaptor<AMQP.BasicProperties> argument = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
		verify(channel).basicPublish(eq(expectedExchange), eq(routingKey), eq(mandatory),
				argument.capture(), aryEq(message.getBody()));
		assertEquals("Wrong correlationId in reply", expectedCorrelationId, argument.getValue().getCorrelationId());
	}

	@Test
	public void emptySendTo() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(String.class);

		Channel channel = mock(Channel.class);

		thrown.expect(ReplyFailureException.class);
		thrown.expectCause(Matchers.isA(AmqpException.class));
		listener.onMessage(createTextMessage("content"), channel);
	}

	@Test
	public void invalidSendTo() {
		thrown.expect(IllegalStateException.class);
		thrown.expectMessage("firstDestination");
		thrown.expectMessage("secondDestination");
		createDefaultInstance(String.class);
	}

	@Test
	public void validatePayloadValid() throws Exception {
		String methodName = "validatePayload";

		DefaultMessageHandlerMethodFactory customFactory = new DefaultMessageHandlerMethodFactory();
		customFactory.setValidator(testValidator("invalid value"));
		initializeFactory(customFactory);

		Method method = getListenerMethod(methodName, String.class);
		MessagingMessageListenerAdapter listener = createInstance(customFactory, method);
		Channel channel = mock(Channel.class);
		listener.onMessage(createTextMessage("test"), channel); // test is a valid value
		assertListenerMethodInvocation(sample, methodName);
	}

	@Test
	public void validatePayloadInvalid() throws Exception {
		DefaultMessageHandlerMethodFactory customFactory = new DefaultMessageHandlerMethodFactory();
		customFactory.setValidator(testValidator("invalid value"));

		Method method = getListenerMethod("validatePayload", String.class);
		MessagingMessageListenerAdapter listener = createInstance(customFactory, method);
		Channel channel = mock(Channel.class);

		thrown.expect(ListenerExecutionFailedException.class);
		listener.onMessage(createTextMessage("invalid value"), channel); // test is an invalid value

	}

	// failure scenario

	@Test
	public void invalidPayloadType() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(Integer.class);
		Channel channel = mock(Channel.class);

		thrown.expect(ListenerExecutionFailedException.class);
		thrown.expectCause(Matchers.isA(org.springframework.messaging.converter.MessageConversionException.class));
		thrown.expectMessage(getDefaultListenerMethod(Integer.class).toGenericString()); // ref to method
		listener.onMessage(createTextMessage("test"), channel); // test is not a valid integer
	}

	@Test
	public void invalidMessagePayloadType() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(Message.class);
		Channel channel = mock(Channel.class);

		thrown.expect(ListenerExecutionFailedException.class);
		thrown.expectCause(Matchers.isA(MethodArgumentTypeMismatchException.class));
		listener.onMessage(createTextMessage("test"), channel);  // Message<String> as Message<Integer>
	}

	private MessagingMessageListenerAdapter createInstance(
			DefaultMessageHandlerMethodFactory factory, Method method, MessageListenerContainer container) {
		MethodRabbitListenerEndpoint endpoint = new MethodRabbitListenerEndpoint();
		endpoint.setBean(sample);
		endpoint.setMethod(method);
		endpoint.setMessageHandlerMethodFactory(factory);
		MessagingMessageListenerAdapter messageListener = endpoint.createMessageListener(container);
		return messageListener;
	}

	private MessagingMessageListenerAdapter createInstance(
			DefaultMessageHandlerMethodFactory factory, Method method) {
		return createInstance(factory, method, new SimpleMessageListenerContainer());
	}

	private MessagingMessageListenerAdapter createDefaultInstance(Class<?>... parameterTypes) {
		return createInstance(this.factory, getDefaultListenerMethod(parameterTypes));
	}

	private Method getListenerMethod(String methodName, Class<?>... parameterTypes) {
		Method method = ReflectionUtils.findMethod(RabbitEndpointSampleBean.class, methodName, parameterTypes);
		assertNotNull("no method found with name " + methodName + " and parameters " + Arrays.toString(parameterTypes));
		return method;
	}

	private Method getDefaultListenerMethod(Class<?>... parameterTypes) {
		return getListenerMethod(name.getMethodName(), parameterTypes);
	}

	private void assertDefaultListenerMethodInvocation() {
		assertListenerMethodInvocation(sample, name.getMethodName());
	}

	private void assertListenerMethodInvocation(RabbitEndpointSampleBean bean, String methodName) {
		assertTrue("Method " + methodName + " should have been invoked", bean.invocations.get(methodName));
	}

	private void initializeFactory(DefaultMessageHandlerMethodFactory factory) {
		factory.setBeanFactory(new StaticListableBeanFactory());
		factory.afterPropertiesSet();
	}

	private Validator testValidator(final String invalidValue) {
		return new Validator() {
			@Override
			public boolean supports(Class<?> clazz) {
				return String.class.isAssignableFrom(clazz);
			}

			@Override
			public void validate(Object target, Errors errors) {
				String value = (String) target;
				if (invalidValue.equals(value)) {
					errors.reject("not a valid value");
				}
			}
		};
	}

	private Method getTestMethod() {
		return ReflectionUtils.findMethod(MethodRabbitListenerEndpointTests.class, name.getMethodName());
	}

	static class RabbitEndpointSampleBean {

		private final Map<String, Boolean> invocations = new HashMap<String, Boolean>();

		public void resolveMessageAndSession(org.springframework.amqp.core.Message message, Channel channel) {
			invocations.put("resolveMessageAndSession", true);
			assertNotNull("Message not injected", message);
			assertNotNull("Channel not injected", channel);
		}

		public void resolveGenericMessage(Message<String> message) {
			invocations.put("resolveGenericMessage", true);
			assertNotNull("Generic message not injected", message);
			assertEquals("Wrong message payload", "test", message.getPayload());
		}

		public void resolveHeaderAndPayload(@Payload String content, @Header int myCounter) {
			invocations.put("resolveHeaderAndPayload", true);
			assertEquals("Wrong @Payload resolution", "my payload", content);
			assertEquals("Wrong @Header resolution", 55, myCounter);
		}

		public void resolveCustomHeaderNameAndPayload(@Payload String content, @Header("myCounter") int counter) {
			invocations.put("resolveCustomHeaderNameAndPayload", true);
			assertEquals("Wrong @Payload resolution", "my payload", content);
			assertEquals("Wrong @Header resolution", 24, counter);
		}

		public void resolveHeaders(String content, @Headers Map<String, Object> headers) {
			invocations.put("resolveHeaders", true);
			assertEquals("Wrong payload resolution", "my payload", content);
			assertNotNull("headers not injected", headers);
			assertEquals("Missing AMQP message id header", "abcd-1234", headers.get(AmqpHeaders.MESSAGE_ID));
			assertEquals("Missing custom header", 1234, headers.get("customInt"));
		}

		public void resolveMessageHeaders(MessageHeaders headers) {
			invocations.put("resolveMessageHeaders", true);
			assertNotNull("MessageHeaders not injected", headers);
			assertEquals("Missing AMQP message type header", "myMessageType", headers.get(AmqpHeaders.TYPE));
			assertEquals("Missing custom header", 4567L, (Long) headers.get("customLong"), 0.0);
		}

		public void resolveRabbitMessageHeaderAccessor(AmqpMessageHeaderAccessor headers) {
			invocations.put("resolveRabbitMessageHeaderAccessor", true);
			assertNotNull("MessageHeader accessor not injected", headers);
			assertEquals("Missing AMQP AppID header", "myAppId", headers.getAppId());
			assertEquals("Missing custom header", true, headers.getHeader("customBoolean"));
		}

		public void resolveObjectPayload(MyBean bean) {
			invocations.put("resolveObjectPayload", true);
			assertNotNull("Object payload not injected", bean);
			assertEquals("Wrong content for payload", "myBean name", bean.name);
		}

		public void resolveConvertedPayload(Integer counter) {
			invocations.put("resolveConvertedPayload", true);
			assertNotNull("Payload not injected", counter);
			assertEquals("Wrong content for payload", Integer.valueOf(33), counter);
		}

		public String processAndReply(@Payload String content) {
			invocations.put("processAndReply", true);
			return content;
		}

		public org.springframework.amqp.core.Message processAndReplyWithMessage(
				org.springframework.amqp.core.Message content) {
			invocations.put("processAndReplyWithMessage", true);
			return content;
		}

		public String processAndReplyWithMessageAndStringReply(
				org.springframework.amqp.core.Message content) {
			invocations.put("processAndReplyWithMessageAndStringReply", true);
			return "foo";
		}

		public String processAndReplyUsingReplyTo(String content) {
			invocations.put("processAndReplyUsingReplyTo", true);
			return content;
		}

		@SendTo("replyDestination")
		public String processAndReplyWithSendTo(String content) {
			invocations.put("processAndReplyWithSendTo", true);
			return content;
		}

		@SendTo("")
		public String emptySendTo(String content) {
			invocations.put("emptySendTo", true);
			return content;
		}

		@SendTo({"firstDestination", "secondDestination"})
		public String invalidSendTo(String content) {
			invocations.put("invalidSendTo", true);
			return content;
		}

		public void validatePayload(@Validated String payload) {
			invocations.put("validatePayload", true);
		}

		public void invalidPayloadType(@Payload Integer payload) {
			throw new IllegalStateException("Should never be called.");
		}

		public void invalidMessagePayloadType(Message<Integer> message) {
			throw new IllegalStateException("Should never be called.");
		}

	}


	@SuppressWarnings("serial")
	static class MyBean implements Serializable {
		private String name;

	}

}
