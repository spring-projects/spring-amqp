/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client;

import com.rabbitmq.client.amqp.ConnectionBuilder;
import com.rabbitmq.client.amqp.Environment;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link SingleAmqpConnectionFactory}.
 *
 * @author Movindu Jayathilake
 *
 * @since 4.0.5
 */
class SingleAmqpConnectionFactoryTests {

	@Test
	void setConnectionNameDelegatesToConnectionBuilder() {
		Environment environment = mock();
		ConnectionBuilder connectionBuilder = mock();

		given(environment.connectionBuilder()).willReturn(connectionBuilder);

		SingleAmqpConnectionFactory connectionFactory =
				new SingleAmqpConnectionFactory(environment);

		SingleAmqpConnectionFactory result =
				connectionFactory.setConnectionName("billing-service");

		verify(connectionBuilder).name("billing-service");
		assertThat(result).isSameAs(connectionFactory);
	}

}
