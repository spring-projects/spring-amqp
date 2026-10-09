/*
 * Copyright 2025-present the original author or authors.
 */

package org.springframework.amqp.client;

import org.apache.qpid.protonj2.client.Connection;

/**
 * The contract for AMQP 1.0 Qpid ProtonJ2 {@link Connection} management.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public interface AmqpConnectionFactory {

	Connection getConnection();

}
