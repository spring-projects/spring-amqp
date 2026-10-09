/*
 * Copyright 2025-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client;

import com.rabbitmq.client.amqp.Connection;

/**
 * The contract for RabbitMQ AMQP 1.0 {@link Connection} management.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public interface AmqpConnectionFactory {

	Connection getConnection();

}
