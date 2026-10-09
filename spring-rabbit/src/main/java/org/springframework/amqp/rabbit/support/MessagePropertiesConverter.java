/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.support;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageProperties;

/**
 * Strategy interface for converting between Spring AMQP {@link MessageProperties}
 * and RabbitMQ BasicProperties.
 *
 * @author Mark Fisher
 * @since 1.0
 */
public interface MessagePropertiesConverter {

	MessageProperties toMessageProperties(BasicProperties source, @Nullable Envelope envelope, String charset);

	BasicProperties fromMessageProperties(MessageProperties source, String charset);

}
