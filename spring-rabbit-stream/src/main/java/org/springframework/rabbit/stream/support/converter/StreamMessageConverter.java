/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.rabbit.stream.support.converter;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.rabbit.stream.support.StreamMessageProperties;

/**
 * Converts between {@link com.rabbitmq.stream.Message} and
 * {@link org.springframework.amqp.core.Message}.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public interface StreamMessageConverter extends MessageConverter {

	Message toMessage(Object object, StreamMessageProperties messageProperties) throws MessageConversionException;

	@Override
	default Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
		throw new UnsupportedOperationException();
	}

	@Override
	com.rabbitmq.stream.Message fromMessage(Message message) throws MessageConversionException;

}
