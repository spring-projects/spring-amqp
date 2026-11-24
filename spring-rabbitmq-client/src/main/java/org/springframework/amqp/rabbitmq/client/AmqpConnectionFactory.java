/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
