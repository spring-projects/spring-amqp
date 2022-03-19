/*
 * Copyright 2002-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.connection;

import org.springframework.amqp.AmqpException;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

/**
 * a {@link ConnectionFactory} that delegates all calls to another ConnectionFactory
 * @author Leonardo Ferreira
 */
public class DelegatingConnectionFactory implements ConnectionFactory {

	private final ConnectionFactory delegate;

	public DelegatingConnectionFactory(@NonNull ConnectionFactory delegate) {
		Assert.notNull(delegate, "delegate cant be null");
		this.delegate = delegate;
	}

	@Override
	public Connection createConnection() throws AmqpException {
		return this.delegate.createConnection();
	}

	@Override
	public String getHost() {
		return this.delegate.getHost();
	}

	@Override
	public int getPort() {
		return this.delegate.getPort();
	}

	@Override
	public String getVirtualHost() {
		return this.delegate.getVirtualHost();
	}

	@Override
	public String getUsername() {
		return this.delegate.getUsername();
	}

	@Override
	public void addConnectionListener(ConnectionListener listener) {
		this.delegate.addConnectionListener(listener);
	}

	@Override
	public boolean removeConnectionListener(ConnectionListener listener) {
		return this.delegate.removeConnectionListener(listener);
	}

	@Override
	public void clearConnectionListeners() {
		this.delegate.clearConnectionListeners();
	}

	@Override
	public ConnectionFactory getPublisherConnectionFactory() {
		return this.delegate.getPublisherConnectionFactory();
	}

	@Override
	public boolean isSimplePublisherConfirms() {
		return this.delegate.isSimplePublisherConfirms();
	}

	@Override
	public boolean isPublisherConfirms() {
		return this.delegate.isPublisherConfirms();
	}

	@Override
	public boolean isPublisherReturns() {
		return this.delegate.isPublisherReturns();
	}

}
