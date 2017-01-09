/*
 * Copyright 2002-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.hamcrest.core.StringContains;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.test.MessageTestUtils;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.MessagingMessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.support.MessageBuilder;


/**
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class RabbitMessagingTemplateTests {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	@Captor
	private ArgumentCaptor<org.springframework.amqp.core.Message> amqpMessage;

	@Mock
	private RabbitTemplate rabbitTemplate;

	private RabbitMessagingTemplate messagingTemplate;


	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
		messagingTemplate = new RabbitMessagingTemplate(rabbitTemplate);
	}

	@Test
	public void validateRabbitTemplate() {
		assertSame(this.rabbitTemplate, messagingTemplate.getRabbitTemplate());
		this.rabbitTemplate.afterPropertiesSet();
	}

	@Test
	public void verifyConverter() {
		RabbitTemplate template = new RabbitTemplate(mock(ConnectionFactory.class));
		RabbitMessagingTemplate rmt = new RabbitMessagingTemplate(template);
		rmt.afterPropertiesSet();
		assertSame(template.getMessageConverter(),
				TestUtils.getPropertyValue(rmt, "amqpMessageConverter.payloadConverter"));

		rmt = new RabbitMessagingTemplate(template);
		MessagingMessageConverter amqpMessageConverter = new MessagingMessageConverter();
		MessageConverter payloadConverter = mock(MessageConverter.class);
		amqpMessageConverter.setPayloadConverter(payloadConverter);
		rmt.setAmqpMessageConverter(amqpMessageConverter);
		rmt.afterPropertiesSet();
		assertSame(payloadConverter, TestUtils.getPropertyValue(rmt, "amqpMessageConverter.payloadConverter"));
	}

	@Test
	public void send() {
		Message<String> message = createTextMessage();

		messagingTemplate.send("myQueue", message);
		verify(rabbitTemplate).send(eq("myQueue"), this.amqpMessage.capture());
		assertTextMessage(this.amqpMessage.getValue());
	}

	@Test
	public void sendExchange() {
		Message<String> message = createTextMessage();

		messagingTemplate.send("myExchange", "myQueue", message);
		verify(rabbitTemplate).send(eq("myExchange"), eq("myQueue"), this.amqpMessage.capture());
		assertTextMessage(this.amqpMessage.getValue());
	}

	@Test
	public void sendDefaultDestination() {
		messagingTemplate.setDefaultDestination("default");
		Message<String> message = createTextMessage();

		messagingTemplate.send(message);
		verify(rabbitTemplate).send(eq("default"), this.amqpMessage.capture());
		assertTextMessage(this.amqpMessage.getValue());
	}

	@Test
	public void sendNoDefaultSet() {
		Message<String> message = createTextMessage();

		thrown.expect(IllegalStateException.class);
		messagingTemplate.send(message);
	}

	@Test
	public void sendPropertyInjection() {
		RabbitMessagingTemplate t = new RabbitMessagingTemplate();
		t.setRabbitTemplate(rabbitTemplate);
		t.setDefaultDestination("myQueue");
		t.afterPropertiesSet();
		Message<String> message = createTextMessage();

		t.send(message);
		verify(rabbitTemplate).send(eq("myQueue"), this.amqpMessage.capture());
		assertTextMessage(this.amqpMessage.getValue());
	}

	@Test
	public void convertAndSendPayload() {
		messagingTemplate.convertAndSend("myQueue", "my Payload");
		verify(rabbitTemplate).send(eq("myQueue"), amqpMessage.capture());
		assertEquals("my Payload", MessageTestUtils.extractText(amqpMessage.getValue()));
	}

	@Test
	public void convertAndSendPayloadExchange() {
		messagingTemplate.convertAndSend("myExchange", "myQueue", "my Payload");
		verify(rabbitTemplate).send(eq("myExchange"), eq("myQueue"), amqpMessage.capture());
		assertEquals("my Payload", MessageTestUtils.extractText(amqpMessage.getValue()));
	}

	@Test
	public void convertAndSendDefaultDestination() {
		messagingTemplate.setDefaultDestination("default");

		messagingTemplate.convertAndSend("my Payload");
		verify(rabbitTemplate).send(eq("default"), amqpMessage.capture());
		assertEquals("my Payload", MessageTestUtils.extractText(amqpMessage.getValue()));
	}

	@Test
	public void convertAndSendNoDefaultSet() {
		thrown.expect(IllegalStateException.class);
		messagingTemplate.convertAndSend("my Payload");
	}

	@Test
	public void convertAndSendCustomAmqpMessageConverter() {
		messagingTemplate.setAmqpMessageConverter(new SimpleMessageConverter() {

			@Override
			protected org.springframework.amqp.core.Message createMessage(Object object,
					MessageProperties messageProperties) throws MessageConversionException {
				throw new MessageConversionException("Test exception");
			}
		});

		thrown.expect(org.springframework.messaging.converter.MessageConversionException.class);
		thrown.expectMessage(new StringContains("Test exception"));
		messagingTemplate.convertAndSend("myQueue", "msg to convert");
	}

	@Test
	public void convertAndSendPayloadAndHeaders() {
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("foo", "bar");

		messagingTemplate.convertAndSend("myQueue", (Object) "Hello", headers);
		verify(rabbitTemplate).send(eq("myQueue"), amqpMessage.capture());
		assertTextMessage(amqpMessage.getValue());
	}

	@Test
	public void convertAndSendPayloadAndHeadersExchange() {
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("foo", "bar");

		messagingTemplate.convertAndSend("myExchange", "myQueue", "Hello", headers);
		verify(rabbitTemplate).send(eq("myExchange"), eq("myQueue"), amqpMessage.capture());
		assertTextMessage(amqpMessage.getValue());
	}

	@Test
	public void receive() {
		org.springframework.amqp.core.Message amqpMessage = createAmqpTextMessage();
		given(rabbitTemplate.receive("myQueue")).willReturn(amqpMessage);

		Message<?> message = messagingTemplate.receive("myQueue");
		verify(rabbitTemplate).receive("myQueue");
		assertTextMessage(message);
	}

	@Test
	public void receiveDefaultDestination() {
		messagingTemplate.setDefaultDestination("default");

		org.springframework.amqp.core.Message amqpMessage = createAmqpTextMessage();
		given(rabbitTemplate.receive("default")).willReturn(amqpMessage);

		Message<?> message = messagingTemplate.receive();
		verify(rabbitTemplate).receive("default");
		assertTextMessage(message);
	}

	@Test
	public void receiveNoDefaultSet() {
		thrown.expect(IllegalStateException.class);
		messagingTemplate.receive();
	}

	@Test
	public void receiveAndConvert() {
		org.springframework.amqp.core.Message amqpMessage = createAmqpTextMessage("my Payload");
		given(rabbitTemplate.receive("myQueue")).willReturn(amqpMessage);


		String payload = messagingTemplate.receiveAndConvert("myQueue", String.class);
		assertEquals("my Payload", payload);
		verify(rabbitTemplate).receive("myQueue");
	}

	@Test
	public void receiveAndConvertDefaultDestination() {
		messagingTemplate.setDefaultDestination("default");

		org.springframework.amqp.core.Message amqpMessage = createAmqpTextMessage("my Payload");
		given(rabbitTemplate.receive("default")).willReturn(amqpMessage);


		String payload = messagingTemplate.receiveAndConvert(String.class);
		assertEquals("my Payload", payload);
		verify(rabbitTemplate).receive("default");
	}

	@Test
	public void receiveAndConvertWithConversion() {
		org.springframework.amqp.core.Message amqpMessage = createAmqpTextMessage("123");
		given(rabbitTemplate.receive("myQueue")).willReturn(amqpMessage);

		messagingTemplate.setMessageConverter(new GenericMessageConverter());

		Integer payload = messagingTemplate.receiveAndConvert("myQueue", Integer.class);
		assertEquals(Integer.valueOf(123), payload);
		verify(rabbitTemplate).receive("myQueue");
	}

	@Test
	public void receiveAndConvertNoConverter() {
		org.springframework.amqp.core.Message amqpMessage = createAmqpTextMessage("Hello");
		given(rabbitTemplate.receive("myQueue")).willReturn(amqpMessage);

		thrown.expect(org.springframework.messaging.converter.MessageConversionException.class);
		messagingTemplate.receiveAndConvert("myQueue", Writer.class);
	}

	@Test
	public void receiveAndConvertNoInput() {
		given(rabbitTemplate.receive("myQueue")).willReturn(null);

		assertNull(messagingTemplate.receiveAndConvert("myQueue", String.class));
	}

	@Test
	public void sendAndReceive() {
		Message<String> request = createTextMessage();
		org.springframework.amqp.core.Message reply = createAmqpTextMessage();
		given(rabbitTemplate.sendAndReceive(eq("myQueue"), anyAmqpMessage())).willReturn(reply);

		Message<?> actual = messagingTemplate.sendAndReceive("myQueue", request);
		verify(rabbitTemplate, times(1)).sendAndReceive(eq("myQueue"),
				anyAmqpMessage());
		assertTextMessage(actual);
	}

	@Test
	public void sendAndReceiveExchange() {
		Message<String> request = createTextMessage();
		org.springframework.amqp.core.Message reply = createAmqpTextMessage();
		given(rabbitTemplate.sendAndReceive(eq("myExchange"), eq("myQueue"), anyAmqpMessage())).willReturn(reply);

		Message<?> actual = messagingTemplate.sendAndReceive("myExchange", "myQueue", request);
		verify(rabbitTemplate, times(1)).sendAndReceive(eq("myExchange"), eq("myQueue"),
				anyAmqpMessage());
		assertTextMessage(actual);
	}

	@Test
	public void sendAndReceiveDefaultDestination() {
		messagingTemplate.setDefaultDestination("default");

		Message<String> request = createTextMessage();
		org.springframework.amqp.core.Message reply = createAmqpTextMessage();
		given(rabbitTemplate.sendAndReceive(eq("default"), anyAmqpMessage())).willReturn(reply);

		Message<?> actual = messagingTemplate.sendAndReceive(request);
		verify(rabbitTemplate, times(1)).sendAndReceive(eq("default"),
				anyAmqpMessage());
		assertTextMessage(actual);
	}

	@Test
	public void sendAndReceiveNoDefaultSet() {
		Message<String> message = createTextMessage();

		thrown.expect(IllegalStateException.class);
		messagingTemplate.sendAndReceive(message);
	}

	@Test
	public void convertSendAndReceivePayload() {
		org.springframework.amqp.core.Message replyMessage = createAmqpTextMessage("My reply");
		given(rabbitTemplate.sendAndReceive(eq("myQueue"), anyAmqpMessage())).willReturn(replyMessage);

		String reply = messagingTemplate.convertSendAndReceive("myQueue", "my Payload", String.class);
		verify(rabbitTemplate, times(1)).sendAndReceive(eq("myQueue"), anyAmqpMessage());
		assertEquals("My reply", reply);
	}

	@Test
	public void convertSendAndReceivePayloadExchange() {
		org.springframework.amqp.core.Message replyMessage = createAmqpTextMessage("My reply");
		given(rabbitTemplate.sendAndReceive(eq("myExchange"), eq("myQueue"), anyAmqpMessage())).willReturn(replyMessage);

		String reply = messagingTemplate.convertSendAndReceive("myExchange", "myQueue", "my Payload", String.class);
		verify(rabbitTemplate, times(1)).sendAndReceive(eq("myExchange"), eq("myQueue"), anyAmqpMessage());
		assertEquals("My reply", reply);
	}

	@Test
	public void convertSendAndReceiveDefaultDestination() {
		messagingTemplate.setDefaultDestination("default");

		org.springframework.amqp.core.Message replyMessage = createAmqpTextMessage("My reply");
		given(rabbitTemplate.sendAndReceive(eq("default"), anyAmqpMessage())).willReturn(replyMessage);

		String reply = messagingTemplate.convertSendAndReceive("my Payload", String.class);
		verify(rabbitTemplate, times(1)).sendAndReceive(eq("default"), anyAmqpMessage());
		assertEquals("My reply", reply);
	}

	@Test
	public void convertSendAndReceiveNoDefaultSet() {
		thrown.expect(IllegalStateException.class);
		messagingTemplate.convertSendAndReceive("my Payload", String.class);
	}

	@Test
	public void convertMessageConversionExceptionOnSend() {
		Message<String> message = createTextMessage();
		MessageConverter messageConverter = mock(MessageConverter.class);
		willThrow(org.springframework.amqp.support.converter.MessageConversionException.class)
				.given(messageConverter).toMessage(eq(message), anyMessageProperties());
		messagingTemplate.setAmqpMessageConverter(messageConverter);

		thrown.expect(org.springframework.messaging.converter.MessageConversionException.class);
		messagingTemplate.send("myQueue", message);
	}

	@Test
	public void convertMessageConversionExceptionOnReceive() {
		org.springframework.amqp.core.Message message = createAmqpTextMessage();
		MessageConverter messageConverter = mock(MessageConverter.class);
		willThrow(org.springframework.amqp.support.converter.MessageConversionException.class)
				.given(messageConverter).fromMessage(message);
		messagingTemplate.setAmqpMessageConverter(messageConverter);
		given(rabbitTemplate.receive("myQueue")).willReturn(message);

		thrown.expect(org.springframework.messaging.converter.MessageConversionException.class);
		messagingTemplate.receive("myQueue");
	}

	private Message<String> createTextMessage(String payload) {
		return MessageBuilder
				.withPayload(payload).setHeader("foo", "bar").build();
	}

	private Message<String> createTextMessage() {
		return createTextMessage("Hello");
	}


	private org.springframework.amqp.core.Message createAmqpTextMessage(String payload) {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("foo", "bar");
		return MessageTestUtils.createTextMessage(payload, properties);

	}

	private org.springframework.amqp.core.Message createAmqpTextMessage() {
		return createAmqpTextMessage("Hello");
	}

	private void assertTextMessage(org.springframework.amqp.core.Message amqpMessage) {
		assertEquals("Wrong body message", "Hello", MessageTestUtils.extractText(amqpMessage));
		assertEquals("Invalid foo property", "bar", amqpMessage.getMessageProperties().getHeaders().get("foo"));
	}

	private void assertTextMessage(Message<?> message) {
		assertNotNull("message should not be null", message);
		assertEquals("Wrong payload", "Hello", message.getPayload());
		assertEquals("Invalid foo property", "bar", message.getHeaders().get("foo"));
	}


	private static org.springframework.amqp.core.Message anyAmqpMessage() {
		return any(org.springframework.amqp.core.Message.class);
	}

	private static MessageProperties anyMessageProperties() {
		return any(MessageProperties.class);
	}

}
