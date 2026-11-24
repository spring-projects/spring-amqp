/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbit.core;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionListener;

/**
 * A {@link ConnectionListener} that will declare a single exchange when the
 * connection is established.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.5.4
 *
 */
public final class DeclareExchangeConnectionListener implements ConnectionListener {

	private final Exchange exchange;

	private final RabbitAdmin admin;

	public DeclareExchangeConnectionListener(Exchange exchange, RabbitAdmin admin) {
		this.exchange = exchange;
		this.admin = admin;
	}

	@Override
	public void onCreate(@Nullable Connection connection) {
		try {
			this.admin.declareExchange(this.exchange);
		}
		catch (Exception e) {
			// Ignore
		}
	}

}
