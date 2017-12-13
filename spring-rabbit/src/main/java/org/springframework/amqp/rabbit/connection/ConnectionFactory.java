/*
 * Copyright 2002-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import org.springframework.amqp.AmqpException;
import org.springframework.lang.Nullable;

/**
 * An interface based ConnectionFactory for creating {@link com.rabbitmq.client.Connection Connections}.
 *
 * <p>
 * NOTE: The Rabbit API contains a ConnectionFactory class (same name).
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 */
public interface ConnectionFactory {

	Connection createConnection() throws AmqpException;

	String getHost();

	int getPort();

	String getVirtualHost();

	String getUsername();

	void addConnectionListener(ConnectionListener listener);

	boolean removeConnectionListener(ConnectionListener listener);

	void clearConnectionListeners();

	/**
	 * Return a separate connection factory for publishers (if implemented).
	 * @return the publisher connection factory, or null.
	 * @since 2.0.2
	 */
	default @Nullable ConnectionFactory getPublisherConnectionFactory() {
		return null;
	}

}
