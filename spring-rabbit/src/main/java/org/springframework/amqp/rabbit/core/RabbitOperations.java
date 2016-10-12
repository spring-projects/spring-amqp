/*
 * Copyright 2002-2010 the original author or authors.
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

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

/**
 * Rabbit specific methods for Amqp functionality.
 * @author Mark Pollack
 * @author Mark Fisher
 */
public interface RabbitOperations extends AmqpTemplate {

	/**
	 * Execute the callback with a channel and reliably close the channel
	 * afterwards.
	 * @param action the call back.
	 * @param <T> the return type.
	 * @return the result from the {@link ChannelCallback#doInRabbit(com.rabbitmq.client.Channel)}.
	 * @throws AmqpException if one occurs.
	 */
	<T> T execute(ChannelCallback<T> action) throws AmqpException;

	/**
	 * Return the connection factory for this operations.
	 * @return the connection factory.
	 * @since 2.0
	 */
	ConnectionFactory getConnectionFactory();

}
