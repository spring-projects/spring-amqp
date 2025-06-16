/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.support.converter;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;


/**
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class MessagingMessageConverterTests {

	private final MessagingMessageConverter converter = new MessagingMessageConverter();

	@Test
	public void onlyHandlesMessage() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> converter.toMessage(new Object(), new MessageProperties()));
	}

	@Test
	public void toMessageWithTextMessage() {
		org.springframework.amqp.core.Message message = converter
				.toMessage(MessageBuilder.withPayload("Hello World").build(), new MessageProperties());

		assertThat(message.getMessageProperties().getContentType()).isEqualTo(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		assertThat(new String(message.getBody())).isEqualTo("Hello World");
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
		assertThat(msg.getPayload()).isEqualTo(1224L);
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
		assertThat(msg.getPayload()).isEqualTo(message.getPayload());
		assertThat(msg.getHeaders().get("inside")).isEqualTo(true);
	}

	public org.springframework.amqp.core.Message createTextMessage(String body) {
		MessageProperties properties = new MessageProperties();
		properties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		return new org.springframework.amqp.core.Message(body.getBytes(), properties);
	}

}
