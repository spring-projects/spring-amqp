/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
