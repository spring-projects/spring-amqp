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

package org.springframework.amqp.rabbit.listener;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.MessageListenerContainer;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.listener.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.ReplyFailureException;
import org.springframework.amqp.rabbit.test.MessageTestUtils;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.AmqpMessageHeaderAccessor;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Stephane Nicoll
 * @author Artem Bilan
 * @author Gary Russell
 * @author Ngoc Nhan
 */
public class MethodRabbitListenerEndpointTests {

	private final DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();

	private final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();

	private final RabbitEndpointSampleBean sample = new RabbitEndpointSampleBean();

	public String testName;

	@BeforeAll
	static void setUp() {
		System.setProperty("spring.amqp.deserialization.trust.all", "true");
	}

	@AfterAll
	static void tearDown() {
		System.setProperty("spring.amqp.deserialization.trust.all", "false");
	}

	@BeforeEach
	public void setup(TestInfo info) {
		initializeFactory(factory);
		this.testName = info.getTestMethod().get().getName();
	}

	@Test
	public void createMessageListener(TestInfo info) {
		MethodRabbitListenerEndpoint endpoint = new MethodRabbitListenerEndpoint();
		endpoint.setBean(this);
		endpoint.setMethod(info.getTestMethod().get());
		endpoint.setMessageHandlerMethodFactory(factory);

		assertThat(endpoint.createMessageListener(container)).isNotNull();
	}

	@Test
	public void resolveMessageAndSession() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(
				org.springframework.amqp.core.Message.class, Channel.class);

		Channel channel = mock(Channel.class);
		listener.onMessage(MessageTestUtils.createTextMessage("test"), channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void resolveGenericMessage() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(Message.class);

		Channel channel = mock(Channel.class);
		listener.onMessage(MessageTestUtils.createTextMessage("test"), channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void resolveHeaderAndPayload() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(String.class, int.class, String.class, String.class);

		Channel channel = mock(Channel.class);
		MessageProperties properties = new MessageProperties();
		properties.setHeader("myCounter", 55);
		properties.setConsumerTag("consumerTag");
		properties.setConsumerQueue("queue");
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("my payload", properties);
		listener.onMessage(message, channel);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void resolveCustomHeaderNameAndPayload() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(String.class, int.class);

		Channel channel = mock(Channel.class);
		MessageProperties properties = new MessageProperties();
		properties.setHeader("myCounter", 24);
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("my payload", properties);
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
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("my payload", properties);
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
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("my payload", properties);
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
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage("my payload", properties);
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

		listener.onMessage(MessageTestUtils.createTextMessage("33"), channel);
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
		properties.setCorrelationId(correlationId);
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage(body, properties);

		processAndReply(listener, message, responseExchange, responseRoutingKey, false, correlationId);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void processAndReplyWithMessage() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(org.springframework.amqp.core.Message.class);
		listener.setMessageConverter(null);
		listener.setResponseExchange("fooQueue");
		String body = "echo text";

		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage(body, new MessageProperties());

		processAndReply(listener, message, "fooQueue", "", false, null);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void processAndReplyWithMessageAndStringReply() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(org.springframework.amqp.core.Message.class);
		listener.setMessageConverter(null);
		listener.setResponseExchange("fooQueue");
		String body = "echo text";

		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage(body, new MessageProperties());

		assertThatExceptionOfType(ReplyFailureException.class)
				.isThrownBy(() -> processAndReply(listener, message, "fooQueue", "", false, null))
				.withCauseInstanceOf(MessageConversionException.class)
				.withMessageContaining("foo");
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void processAndReplyUsingReplyTo() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(String.class);
		listener.setMandatoryPublish(true);
		String body = "echo text";
		Address replyTo = new Address("replyToQueue", "myRouting");

		MessageProperties properties = new MessageProperties();
		properties.setReplyToAddress(replyTo);
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage(body, properties);

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
		org.springframework.amqp.core.Message message = MessageTestUtils.createTextMessage(body, properties);

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
		assertThat(argument.getValue().getCorrelationId()).as("Wrong correlationId in reply").isEqualTo(expectedCorrelationId);
	}

	@Test
	public void emptySendTo() throws Exception {
		MessagingMessageListenerAdapter listener = createDefaultInstance(String.class);

		processAndReply(listener, MessageTestUtils.createTextMessage("content"), "", "", false, null);
		assertDefaultListenerMethodInvocation();
	}

	@Test
	public void noSendToValue() throws Exception {
		emptySendTo();
	}

	@Test
	public void invalidSendTo() {
		assertThatIllegalStateException()
				.isThrownBy(() -> createDefaultInstance(String.class))
				.withMessageMatching(".*firstDestination, secondDestination.*");
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
		listener.onMessage(MessageTestUtils.createTextMessage("test"), channel); // test is a valid value
		assertListenerMethodInvocation(sample, methodName);
	}

	@Test
	public void validatePayloadInvalid() {
		DefaultMessageHandlerMethodFactory customFactory = new DefaultMessageHandlerMethodFactory();
		customFactory.setValidator(testValidator("invalid value"));

		Method method = getListenerMethod("validatePayload", String.class);
		MessagingMessageListenerAdapter listener = createInstance(customFactory, method);
		Channel channel = mock(Channel.class);

		assertThatThrownBy(() -> listener.onMessage(MessageTestUtils.createTextMessage("invalid value"), channel))
				.isInstanceOf(ListenerExecutionFailedException.class);

	}

	// failure scenario

	@Test
	public void invalidPayloadType() {
		MessagingMessageListenerAdapter listener = createDefaultInstance(Integer.class);
		Channel channel = mock(Channel.class);

		// test is not a valid integer
		assertThatThrownBy(() -> listener.onMessage(MessageTestUtils.createTextMessage("test"), channel))
				.isInstanceOf(ListenerExecutionFailedException.class)
				.hasCauseExactlyInstanceOf(org.springframework.messaging.converter.MessageConversionException.class)
				.hasMessageContaining(getDefaultListenerMethod(Integer.class).toGenericString()); // ref to method
	}

	@Test
	public void invalidMessagePayloadType() {
		MessagingMessageListenerAdapter listener = createDefaultInstance(Message.class);
		Channel channel = mock(Channel.class);

		// Message<String> as Message<Integer>
		assertThatThrownBy(() -> listener.onMessage(MessageTestUtils.createTextMessage("test"), channel))
				.extracting(t -> t.getCause())
				.isInstanceOfAny(MethodArgumentTypeMismatchException.class,
						org.springframework.messaging.converter.MessageConversionException.class);
	}

	private MessagingMessageListenerAdapter createInstance(
			DefaultMessageHandlerMethodFactory methodFactory, Method method,
			MessageListenerContainer listenerContainer) {

		MethodRabbitListenerEndpoint endpoint = new MethodRabbitListenerEndpoint();
		endpoint.setBean(sample);
		endpoint.setMethod(method);
		endpoint.setMessageHandlerMethodFactory(methodFactory);
		return endpoint.createMessageListener(listenerContainer);
	}

	private MessagingMessageListenerAdapter createInstance(
			DefaultMessageHandlerMethodFactory methodFactory, Method method) {

		return createInstance(methodFactory, method, new SimpleMessageListenerContainer());
	}

	private MessagingMessageListenerAdapter createDefaultInstance(Class<?>... parameterTypes) {
		return createInstance(this.factory, getDefaultListenerMethod(parameterTypes));
	}

	private Method getListenerMethod(String methodName, Class<?>... parameterTypes) {
		Method method = ReflectionUtils.findMethod(RabbitEndpointSampleBean.class, methodName, parameterTypes);
		assertThat("no method found with name " + methodName + " and parameters " + Arrays.toString(parameterTypes)).isNotNull();
		return method;
	}

	private Method getDefaultListenerMethod(Class<?>... parameterTypes) {
		return getListenerMethod(this.testName, parameterTypes);
	}

	private void assertDefaultListenerMethodInvocation() {
		assertListenerMethodInvocation(this.sample, this.testName);
	}

	private void assertListenerMethodInvocation(RabbitEndpointSampleBean bean, String methodName) {
		assertThat(bean.invocations.get(methodName)).as("Method " + methodName + " should have been invoked").isTrue();
	}

	private void initializeFactory(DefaultMessageHandlerMethodFactory methodFactory) {
		methodFactory.setBeanFactory(new StaticListableBeanFactory());
		methodFactory.afterPropertiesSet();
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

	static class RabbitEndpointSampleBean {

		private final Map<String, Boolean> invocations = new HashMap<String, Boolean>();

		public void resolveMessageAndSession(org.springframework.amqp.core.Message message, Channel channel) {
			invocations.put("resolveMessageAndSession", true);
			assertThat(message).as("Message not injected").isNotNull();
			assertThat(channel).as("Channel not injected").isNotNull();
		}

		public void resolveGenericMessage(Message<String> message) {
			invocations.put("resolveGenericMessage", true);
			assertThat(message).as("Generic message not injected").isNotNull();
			assertThat(message.getPayload()).as("Wrong message payload").isEqualTo("test");
		}

		public void resolveHeaderAndPayload(@Payload String content, @Header("myCounter") int myCounter,
				@Header(AmqpHeaders.CONSUMER_TAG) String tag,
				@Header(AmqpHeaders.CONSUMER_QUEUE) String queue) {
			invocations.put("resolveHeaderAndPayload", true);
			assertThat(content).as("Wrong @Payload resolution").isEqualTo("my payload");
			assertThat(myCounter).as("Wrong @Header resolution").isEqualTo(55);
			assertThat(tag).as("Wrong consumer tag header").isEqualTo("consumerTag");
			assertThat(queue).as("Wrong queue header").isEqualTo("queue");
		}

		public void resolveCustomHeaderNameAndPayload(@Payload String content, @Header("myCounter") int counter) {
			invocations.put("resolveCustomHeaderNameAndPayload", true);
			assertThat(content).as("Wrong @Payload resolution").isEqualTo("my payload");
			assertThat(counter).as("Wrong @Header resolution").isEqualTo(24);
		}

		public void resolveHeaders(String content, @Headers Map<String, Object> headers) {
			invocations.put("resolveHeaders", true);
			assertThat(content).as("Wrong payload resolution").isEqualTo("my payload");
			assertThat(headers).as("headers not injected").isNotNull();
			assertThat(headers.get(AmqpHeaders.MESSAGE_ID)).as("Missing AMQP message id header").isEqualTo("abcd-1234");
			assertThat(headers.get("customInt")).as("Missing custom header").isEqualTo(1234);
		}

		public void resolveMessageHeaders(MessageHeaders headers) {
			invocations.put("resolveMessageHeaders", true);
			assertThat(headers).as("MessageHeaders not injected").isNotNull();
			assertThat(headers.get(AmqpHeaders.TYPE)).as("Missing AMQP message type header").isEqualTo("myMessageType");
			assertThat(headers.get("customLong", Long.class)).as("Missing custom header").isEqualTo(4567L);
		}

		public void resolveRabbitMessageHeaderAccessor(AmqpMessageHeaderAccessor headers) {
			invocations.put("resolveRabbitMessageHeaderAccessor", true);
			assertThat(headers).as("MessageHeader accessor not injected").isNotNull();
			assertThat(headers.getAppId()).as("Missing AMQP AppID header").isEqualTo("myAppId");
			assertThat(headers.getHeader("customBoolean")).as("Missing custom header").isEqualTo(Boolean.TRUE);
		}

		public void resolveObjectPayload(MyBean bean) {
			invocations.put("resolveObjectPayload", true);
			assertThat(bean).as("Object payload not injected").isNotNull();
			assertThat(bean.name).as("Wrong content for payload").isEqualTo("myBean name");
		}

		public void resolveConvertedPayload(Integer counter) {
			invocations.put("resolveConvertedPayload", true);
			assertThat(counter).as("Payload not injected").isNotNull();
			assertThat(counter).as("Wrong content for payload").isEqualTo(33);
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

		@SendTo("replyDestination/")
		public String processAndReplyWithSendTo(String content) {
			invocations.put("processAndReplyWithSendTo", true);
			return content;
		}

		@SendTo("")
		public String emptySendTo(String content) {
			invocations.put("emptySendTo", true);
			return content;
		}

		@SendTo
		public String noSendToValue(String content) {
			invocations.put("noSendToValue", true);
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
