/*
 * Copyright 2017-present the original author or authors.
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
