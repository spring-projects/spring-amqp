/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.PublisherBuilder;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

/**
 * @author Sharang Gupta
 *
 * @since 4.0.7
 */
public class RabbitAmqpTemplateUnitTests {

	private final Connection connection = mock();

	@Test
	void temporaryPublisherIsClosedWhenPublishFails() {
		AmqpConnectionFactory connectionFactory =
				mock(AmqpConnectionFactory.class, withSettings().extraInterfaces(RoutingAmqpConnectionFactory.class));
		given(connectionFactory.getConnection()).willReturn(this.connection);
		PublisherBuilder publisherBuilder = mock(PublisherBuilder.class, RETURNS_SELF);
		Publisher publisher = mock();
		given(this.connection.publisherBuilder()).willReturn(publisherBuilder);
		given(publisherBuilder.build()).willReturn(publisher);
		given(publisher.message()).willReturn(mock(com.rabbitmq.client.amqp.Message.class, RETURNS_DEEP_STUBS));
		willThrow(new AmqpException("The publisher is closed")).given(publisher).publish(any(), any());
		RabbitAmqpTemplate template = new RabbitAmqpTemplate(connectionFactory);

		assertThatExceptionOfType(AmqpException.class)
				.isThrownBy(() -> template.send("test.queue", new Message("data".getBytes())));

		verify(publisher).close();
	}

}
