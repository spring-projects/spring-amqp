/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbit.connection;

import org.springframework.amqp.event.AmqpEvent;

/**
 * The {@link AmqpEvent} emitted by the {@code CachingConnectionFactory}
 * when its connections are unblocked.
 *
 * @author Artem Bilan
 *
 * @since 2.0
 *
 * @see com.rabbitmq.client.BlockedListener#handleUnblocked()
 */
@SuppressWarnings("serial")
public class ConnectionUnblockedEvent extends AmqpEvent {

	public ConnectionUnblockedEvent(Connection source) {
		super(source);
	}

	public Connection getConnection() {
		return (Connection) getSource();
	}

}
