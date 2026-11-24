/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
