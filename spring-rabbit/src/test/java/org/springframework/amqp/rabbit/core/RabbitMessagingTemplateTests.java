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

package org.springframework.amqp.rabbit.core;

import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.test.MessageTestUtils;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.MessagingMessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 */
public class RabbitMessagingTemplateTests {

	@Captor
	private ArgumentCaptor<org.springframework.amqp.core.Message> amqpMessage;

	@Mock
	private RabbitTemplate rabbitTemplate;

	private RabbitMessagingTemplate messagingTemplate;

	private AutoCloseable openMocks;

	@BeforeEach
	public void setup() {
		this.openMocks = MockitoAnnotations.openMocks(this);
		given(this.rabbitTemplate.getMessageConverter()).willReturn(new SimpleMessageConverter());
		messagingTemplate = new RabbitMessagingTemplate(rabbitTemplate);
	}

	@AfterEach
	public void tearDown() throws Exception {
		this.openMocks.close();
	}

	@Test
	public void validateRabbitTemplate() {
		assertThat(messagingTemplate.getRabbitTemplate()).isSameAs(this.rabbitTemplate);
		this.rabbitTemplate.afterPropertiesSet();
	}

	@Test
	public void verifyConverter() {
		RabbitTemplate template = new RabbitTemplate(mock(ConnectionFactory.class));
		RabbitMessagingTemplate rmt = new RabbitMessagingTemplate(template);
		rmt.afterPropertiesSet();
		assertThat(TestUtils.getPropertyValue(rmt, "amqpMessageConverter.payloadConverter"))
				.isSameAs(template.getMessageConverter());

		rmt = new RabbitMessagingTemplate(template);
		MessagingMessageConverter amqpMessageConverter = new MessagingMessageConverter();
		MessageConverter payloadConverter = mock(MessageConverter.class);
		amqpMessageConverter.setPayloadConverter(payloadConverter);
		rmt.setAmqpMessageConverter(amqpMessageConverter);
		rmt.afterPropertiesSet();
		assertThat(TestUtils.getPropertyValue(rmt, "amqpMessageConverter.payloadConverter")).isSameAs(payloadConverter);
	}

	@Test
	void correlation() {
		this.messagingTemplate.setDefaultDestination("defRk");
		this.messagingTemplate.send(new GenericMessage<>("foo",
				Collections.singletonMap(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, new CorrelationData())));
		verify(this.rabbitTemplate).send(eq("defRk"), any(), any(CorrelationData.class));
		this.messagingTemplate.send("rk", new GenericMessage<>("foo",
				Collections.singletonMap(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, new CorrelationData())));
		verify(this.rabbitTemplate).send(eq("rk"), any(), any(CorrelationData.class));
		this.messagingTemplate.send("ex", "rk", new GenericMessage<>("foo",
				Collections.singletonMap(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, new CorrelationData())));
		verify(this.rabbitTemplate).send(eq("ex"), eq("rk"), any(), any(CorrelationData.class));
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

		assertThatIllegalStateException()
			.isThrownBy(() -> messagingTemplate.send(message));
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
		assertThat(MessageTestUtils.extractText(amqpMessage.getValue())).isEqualTo("my Payload");
	}

	@Test
	public void convertAndSendPayloadExchange() {
		messagingTemplate.convertAndSend("myExchange", "myQueue", "my Payload");
		verify(rabbitTemplate).send(eq("myExchange"), eq("myQueue"), amqpMessage.capture());
		assertThat(MessageTestUtils.extractText(amqpMessage.getValue())).isEqualTo("my Payload");
	}

	@Test
	public void convertAndSendDefaultDestination() {
		messagingTemplate.setDefaultDestination("default");

		messagingTemplate.convertAndSend("my Payload");
		verify(rabbitTemplate).send(eq("default"), amqpMessage.capture());
		assertThat(MessageTestUtils.extractText(amqpMessage.getValue())).isEqualTo("my Payload");
	}

	@Test
	public void convertAndSendNoDefaultSet() {
		assertThatIllegalStateException()
			.isThrownBy(() -> messagingTemplate.convertAndSend("my Payload"));
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

		assertThatThrownBy(() -> messagingTemplate.convertAndSend("myQueue", "msg to convert"))
			.isExactlyInstanceOf(org.springframework.messaging.converter.MessageConversionException.class)
			.hasStackTraceContaining("Test exception");
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
		org.springframework.amqp.core.Message amqpMsg = createAmqpTextMessage();
		given(rabbitTemplate.receive("myQueue")).willReturn(amqpMsg);

		Message<?> message = messagingTemplate.receive("myQueue");
		verify(rabbitTemplate).receive("myQueue");
		assertTextMessage(message);
	}

	@Test
	public void receiveDefaultDestination() {
		messagingTemplate.setDefaultDestination("default");

		org.springframework.amqp.core.Message amqpMsg = createAmqpTextMessage();
		given(rabbitTemplate.receive("default")).willReturn(amqpMsg);

		Message<?> message = messagingTemplate.receive();
		verify(rabbitTemplate).receive("default");
		assertTextMessage(message);
	}

	@Test
	public void receiveDefaultDestinationOverride() {
		messagingTemplate.setDefaultDestination("defaultDest");
		messagingTemplate.setUseTemplateDefaultReceiveQueue(true);

		org.springframework.amqp.core.Message amqpMsg = createAmqpTextMessage();
		given(rabbitTemplate.getDefaultReceiveQueue()).willReturn("default");
		given(rabbitTemplate.receive("default")).willReturn(amqpMsg);

		Message<?> message = messagingTemplate.receive();
		verify(rabbitTemplate).receive("default");
		assertTextMessage(message);
	}

	@Test
	public void receiveNoDefaultSet() {
		assertThatIllegalStateException()
			.isThrownBy(() -> messagingTemplate.receive());
	}

	@Test
	public void receiveAndConvert() {
		org.springframework.amqp.core.Message amqpMsg = createAmqpTextMessage("my Payload");
		given(rabbitTemplate.receive("myQueue")).willReturn(amqpMsg);


		String payload = messagingTemplate.receiveAndConvert("myQueue", String.class);
		assertThat(payload).isEqualTo("my Payload");
		verify(rabbitTemplate).receive("myQueue");
	}

	@Test
	public void receiveAndConvertDefaultDestination() {
		messagingTemplate.setDefaultDestination("default");

		org.springframework.amqp.core.Message amqpMsg = createAmqpTextMessage("my Payload");
		given(rabbitTemplate.receive("default")).willReturn(amqpMsg);


		String payload = messagingTemplate.receiveAndConvert(String.class);
		assertThat(payload).isEqualTo("my Payload");
		verify(rabbitTemplate).receive("default");
	}

	@Test
	public void receiveAndConvertWithConversion() {
		org.springframework.amqp.core.Message message = createAmqpTextMessage("123");
		given(rabbitTemplate.receive("myQueue")).willReturn(message);

		messagingTemplate.setMessageConverter(new GenericMessageConverter());

		Integer payload = messagingTemplate.receiveAndConvert("myQueue", Integer.class);
		assertThat(payload).isEqualTo(Integer.valueOf(123));
		verify(rabbitTemplate).receive("myQueue");
	}

	@Test
	public void receiveAndConvertNoConverter() {
		org.springframework.amqp.core.Message message = createAmqpTextMessage("Hello");
		given(rabbitTemplate.receive("myQueue")).willReturn(message);

		assertThatThrownBy(() -> messagingTemplate.receiveAndConvert("myQueue", Writer.class))
			.isInstanceOf(org.springframework.messaging.converter.MessageConversionException.class);
	}

	@Test
	public void receiveAndConvertNoInput() {
		given(rabbitTemplate.receive("myQueue")).willReturn(null);

		assertThat(messagingTemplate.receiveAndConvert("myQueue", String.class)).isNull();
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

		assertThatIllegalStateException()
			.isThrownBy(() -> messagingTemplate.sendAndReceive(message));
	}

	@Test
	public void convertSendAndReceivePayload() {
		org.springframework.amqp.core.Message replyMessage = createAmqpTextMessage("My reply");
		given(rabbitTemplate.sendAndReceive(eq("myQueue"), anyAmqpMessage())).willReturn(replyMessage);

		String reply = messagingTemplate.convertSendAndReceive("myQueue", "my Payload", String.class);
		verify(rabbitTemplate, times(1)).sendAndReceive(eq("myQueue"), anyAmqpMessage());
		assertThat(reply).isEqualTo("My reply");
	}

	@Test
	public void convertSendAndReceivePayloadExchange() {
		org.springframework.amqp.core.Message replyMessage = createAmqpTextMessage("My reply");
		given(rabbitTemplate.sendAndReceive(eq("myExchange"), eq("myQueue"), anyAmqpMessage())).willReturn(replyMessage);

		String reply = messagingTemplate.convertSendAndReceive("myExchange", "myQueue", "my Payload", String.class);
		verify(rabbitTemplate, times(1)).sendAndReceive(eq("myExchange"), eq("myQueue"), anyAmqpMessage());
		assertThat(reply).isEqualTo("My reply");
	}

	@Test
	public void convertSendAndReceiveDefaultDestination() {
		messagingTemplate.setDefaultDestination("default");

		org.springframework.amqp.core.Message replyMessage = createAmqpTextMessage("My reply");
		given(rabbitTemplate.sendAndReceive(eq("default"), anyAmqpMessage())).willReturn(replyMessage);

		String reply = messagingTemplate.convertSendAndReceive("my Payload", String.class);
		verify(rabbitTemplate, times(1)).sendAndReceive(eq("default"), anyAmqpMessage());
		assertThat(reply).isEqualTo("My reply");
	}

	@Test
	public void convertSendAndReceiveNoDefaultSet() {
		assertThatIllegalStateException()
			.isThrownBy(() -> messagingTemplate.convertSendAndReceive("my Payload", String.class));
	}

	@Test
	public void convertMessageConversionExceptionOnSend() {
		Message<String> message = createTextMessage();
		MessageConverter messageConverter = mock(MessageConverter.class);
		willThrow(org.springframework.amqp.support.converter.MessageConversionException.class)
				.given(messageConverter).toMessage(eq(message), anyMessageProperties());
		messagingTemplate.setAmqpMessageConverter(messageConverter);

		assertThatThrownBy(() -> messagingTemplate.send("myQueue", message))
			.isInstanceOf(org.springframework.messaging.converter.MessageConversionException.class);
	}

	@Test
	public void convertMessageConversionExceptionOnReceive() {
		org.springframework.amqp.core.Message message = createAmqpTextMessage();
		MessageConverter messageConverter = mock(MessageConverter.class);
		willThrow(org.springframework.amqp.support.converter.MessageConversionException.class)
				.given(messageConverter).fromMessage(message);
		messagingTemplate.setAmqpMessageConverter(messageConverter);
		given(rabbitTemplate.receive("myQueue")).willReturn(message);

		assertThatThrownBy(() -> messagingTemplate.receive("myQueue"))
			.isInstanceOf(org.springframework.messaging.converter.MessageConversionException.class);
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

	private void assertTextMessage(org.springframework.amqp.core.Message amqpMsg) {
		assertThat(MessageTestUtils.extractText(amqpMsg)).as("Wrong body message").isEqualTo("Hello");
		assertThat(amqpMsg.getMessageProperties().getHeaders().get("foo")).as("Invalid foo property").isEqualTo("bar");
	}

	private void assertTextMessage(Message<?> message) {
		assertThat(message).as("message should not be null").isNotNull();
		assertThat(message.getPayload()).as("Wrong payload").isEqualTo("Hello");
		assertThat(message.getHeaders().get("foo")).as("Invalid foo property").isEqualTo("bar");
	}


	private static org.springframework.amqp.core.Message anyAmqpMessage() {
		return any(org.springframework.amqp.core.Message.class);
	}

	private static MessageProperties anyMessageProperties() {
		return any(MessageProperties.class);
	}

}
