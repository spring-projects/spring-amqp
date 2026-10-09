/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import org.springframework.amqp.event.AmqpEvent;

/**
 * The {@link AmqpEvent} emitted by the {@code CachingConnectionFactory}
 * when its connections are blocked.
 *
 * @author Artem Bilan
 *
 * @since 2.0
 *
 * @see com.rabbitmq.client.BlockedListener#handleBlocked(String)
 */
@SuppressWarnings("serial")
public class ConnectionBlockedEvent extends AmqpEvent {

	private final String reason;

	public ConnectionBlockedEvent(Connection source, String reason) {
		super(source);
		this.reason = reason;
	}

	public Connection getConnection() {
		return (Connection) getSource();
	}

	public String getReason() {
		return this.reason;
	}

}
