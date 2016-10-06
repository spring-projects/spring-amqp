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

package org.springframework.amqp.rabbit.connection;

/**
 * A strategy to build an application-specific connection name,
 * which can be displayed in the management UI if RabbitMQ server supports it.
 * The value doesn't have to be unique and cannot be used
 * as a connection identifier e.g. in HTTP API requests.
 * The value is supposed to be human-readable.
 *
 * @author Artem Bilan
 * @since 2.0
 * @see com.rabbitmq.client.ConnectionFactory#newConnection(com.rabbitmq.client.Address[], String)
 */
@FunctionalInterface
public interface ConnectionNameStrategy {

	String obtainNewConnectionName(ConnectionFactory connectionFactory);

}
