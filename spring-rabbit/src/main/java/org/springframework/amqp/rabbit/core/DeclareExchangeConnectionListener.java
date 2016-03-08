/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.core;

import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionListener;

/**
 * A {@link ConnectionListener} that will declare a single exchange when the
 * connection is established.
 *
 * @author Gary Russell
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
	public void onCreate(Connection connection) {
		try {
			this.admin.declareExchange(this.exchange);
		}
		catch (Exception e) {
		}
	}

	@Override
	public void onClose(Connection connection) {
	}

}
