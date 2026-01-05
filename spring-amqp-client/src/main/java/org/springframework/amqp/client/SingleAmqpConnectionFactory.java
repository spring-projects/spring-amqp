/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.amqp.client;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.DisposableBean;

/**
 * The {@link AmqpConnectionFactory} implementation to hold a single, shared {@link Connection} instance.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class SingleAmqpConnectionFactory implements AmqpConnectionFactory, DisposableBean {

	private final Lock instanceLock = new ReentrantLock();

	private final Client protonjClient;

	private ConnectionOptions connectionOptions = new ConnectionOptions();

	private String host = "localhost";

	private int port = -1;

	private volatile @Nullable Connection connection;

	public SingleAmqpConnectionFactory(Client protonjClient) {
		this.protonjClient = protonjClient;
	}

	public SingleAmqpConnectionFactory setHost(String host) {
		this.host = host;
		return this;
	}

	public SingleAmqpConnectionFactory setPort(int port) {
		this.port = port;
		return this;
	}

	/**
	 * Set the username for the AMQP connection.
	 * The convenient top-level property of the {@link ConnectionOptions}.
	 * If a {@link ConnectionOptions} is provided, the username has to be set over there.
	 * @param username the username to use.
	 * @return the factory instance.
	 */
	public SingleAmqpConnectionFactory setUsername(String username) {
		this.connectionOptions.user(username);
		return this;
	}

	/**
	 * Set the password for the AMQP connection.
	 * The convenient top-level property of the {@link ConnectionOptions}.
	 * If a {@link ConnectionOptions} is provided, the password has to be set over there.
	 * @param password the password to use.
	 * @return the factory instance.
	 */
	public SingleAmqpConnectionFactory setPassword(String password) {
		this.connectionOptions.password(password);
		return this;
	}

	/**
	 * Set a {@link ConnectionOptions} instance.
	 * Mutually exclusive with {@link #setUsername(String)} and {@link #setPassword(String)}.
	 * @param connectionOptions to use.
	 * @return the factory instance.
	 */
	public SingleAmqpConnectionFactory setConnectionOptions(ConnectionOptions connectionOptions) {
		this.connectionOptions = connectionOptions.clone();
		return this;
	}

	@Override
	public Connection getConnection() {
		Connection connectionToReturn = this.connection;
		if (connectionToReturn == null) {
			this.instanceLock.lock();
			try {
				connectionToReturn = this.connection;
				if (connectionToReturn == null) {
					connectionToReturn = this.protonjClient.connect(this.host, this.port, this.connectionOptions);
					this.connection = connectionToReturn;
				}
			}
			catch (ClientException ex) {
				throw ProtonUtils.toAmqpException(ex);
			}
			finally {
				this.instanceLock.unlock();
			}
		}
		return connectionToReturn;
	}

	@Override
	public void destroy() {
		Connection connectionToClose = this.connection;
		if (connectionToClose != null) {
			connectionToClose.close();
			this.connection = null;
		}
	}

}
