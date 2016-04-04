/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.support.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;


/**
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class MessagingMessageConverterTests {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	private final MessagingMessageConverter converter = new MessagingMessageConverter();

	@Test
	public void onlyHandlesMessage() {
		thrown.expect(IllegalArgumentException.class);
		converter.toMessage(new Object(), new MessageProperties());
	}

	@Test
	public void toMessageWithTextMessage() {
		org.springframework.amqp.core.Message message = converter
				.toMessage(MessageBuilder.withPayload("Hello World").build(), new MessageProperties());

		assertEquals(MessageProperties.CONTENT_TYPE_TEXT_PLAIN, message.getMessageProperties().getContentType());
		assertEquals("Hello World", new String(message.getBody()));
	}

	@Test
	public void fromNull() {
		assertNull(converter.fromMessage(null));
	}

	@Test
	public void customPayloadConverter() throws Exception {
		converter.setPayloadConverter(new SimpleMessageConverter() {
			@Override
			public Object fromMessage(org.springframework.amqp.core.Message message) throws MessageConversionException {
				String payload = new String(message.getBody());
				return Long.parseLong(payload);
			}
		});

		Message<?> msg = (Message<?>) converter.fromMessage(createTextMessage("1224"));
		assertEquals(1224L, msg.getPayload());
	}

	@Test
	public void payloadIsAMessage() {
		final Message<String> message = MessageBuilder.withPayload("Test").setHeader("inside", true).build();
		converter.setPayloadConverter(new SimpleMessageConverter() {

			@Override
			public Object fromMessage(org.springframework.amqp.core.Message amqpMessage) throws MessageConversionException {
				return message;
			}
		});
		Message<?> msg = (Message<?>) converter.fromMessage(createTextMessage("foo"));
		assertEquals(message.getPayload(), msg.getPayload());
		assertEquals(true, msg.getHeaders().get("inside"));
	}

	public org.springframework.amqp.core.Message createTextMessage(String body) {
		MessageProperties properties = new MessageProperties();
		properties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		return new org.springframework.amqp.core.Message(body.getBytes(), properties);
	}

}
