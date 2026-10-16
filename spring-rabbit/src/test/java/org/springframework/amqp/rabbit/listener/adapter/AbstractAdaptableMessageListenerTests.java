/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import java.nio.charset.StandardCharsets;

import com.rabbitmq.client.Channel;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.support.converter.MessageConversionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Ngoc Nhan
 */
class AbstractAdaptableMessageListenerTests {

	@Test
	public void buildMessageWithMessageInstanceAndDefaultConverter() {

		AbstractAdaptableMessageListener listener = createListener();

		Message message = new Message("Hello World".getBytes(StandardCharsets.UTF_8));
		Message replyMessage = listener.buildMessage(null, message, null);

		assertThat(replyMessage).as("reply should never be null").isNotNull();
		assertThat(new String(replyMessage.getBody())).isEqualTo("Hello World");
	}

	@Test
	public void buildMessageWithoutConverterThrowsException() {

		AbstractAdaptableMessageListener listener = createListener();
		listener.setMessageConverter(null);

		assertThatExceptionOfType(MessageConversionException.class)
				.isThrownBy(() -> listener.buildMessage(null, null, null));
	}

	@Test
	public void buildMessageWithDefaultConverter() {

		AbstractAdaptableMessageListener listener = createListener();

		Message replyMessage = listener.buildMessage(null, null, null);

		assertThat(replyMessage).as("reply should never be null").isNotNull();
		assertThat(replyMessage.getBody()).isEmpty();
	}

	private AbstractAdaptableMessageListener createListener() {
		return new AbstractAdaptableMessageListener() {
			@Override
			public void onMessage(Message message, @Nullable Channel channel) {
				throw new UnsupportedOperationException();
			}
		};
	}

}
