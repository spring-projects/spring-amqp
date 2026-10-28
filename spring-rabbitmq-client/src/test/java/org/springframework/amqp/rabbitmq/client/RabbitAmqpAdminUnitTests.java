/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Management;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Sharang Gupta
 *
 * @since 4.2
 */
public class RabbitAmqpAdminUnitTests {

	private final AmqpConnectionFactory connectionFactory = mock();

	private final Management management = mock();

	private final RabbitAmqpAdmin admin = new RabbitAmqpAdmin(this.connectionFactory);

	@BeforeEach
	void setUp() {
		Connection connection = mock();
		given(this.connectionFactory.getConnection()).willReturn(connection);
		given(connection.management()).willReturn(this.management);
	}

	@Test
	void deleteQueueReturnsFalseForMissingQueue() {
		given(this.management.queueInfo("missing"))
				.willThrow(new AmqpException.AmqpEntityDoesNotExistException("no such queue"));

		assertThat(this.admin.deleteQueue("missing")).isFalse();

		verify(this.management, never()).queueDelete("missing");
	}

	@Test
	void deleteQueueReturnsTrueForExistingQueue() {
		given(this.management.queueInfo("existing")).willReturn(mock(Management.QueueInfo.class));

		assertThat(this.admin.deleteQueue("existing")).isTrue();

		verify(this.management).queueDelete("existing");
	}

	@Test
	void conditionalDeleteOfMissingQueueIsNoOp() {
		given(this.management.queueInfo("missing"))
				.willThrow(new AmqpException.AmqpEntityDoesNotExistException("no such queue"));

		assertThatNoException().isThrownBy(() -> this.admin.deleteQueue("missing", true, true));

		verify(this.management, never()).queueDelete("missing");
	}

}
