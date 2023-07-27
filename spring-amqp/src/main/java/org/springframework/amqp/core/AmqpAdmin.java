/*
 * Copyright 2002-2023 the original author or authors.
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

package org.springframework.amqp.core;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.lang.Nullable;


/**
 * Specifies a basic set of portable AMQP administrative operations for AMQP &gt; 0.9.
 *
 * @author Mark Pollack
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
public interface AmqpAdmin {

	/**
	 * Declare an exchange.
	 * @param exchange the exchange to declare.
	 */
	void declareExchange(Exchange exchange);

	/**
	 * Delete an exchange. Look at implementation specific subclass for implementation specific behavior, for example
	 * for RabbitMQ this will delete the exchange without regard for whether it is in use or not.
	 * @param exchangeName the name of the exchange
	 * @return true if the exchange existed and was deleted
	 */
	boolean deleteExchange(String exchangeName);

	// Queue Operations

	/**
	 * Declare a queue whose name is automatically named. It is created with exclusive = true, autoDelete=true, and
	 * durable = false.
	 *
	 * @return The queue.
	 */
	@Nullable
	Queue declareQueue();

	/**
	 * Declare the given queue.
	 * @param queue the queue to declare.
	 * @return the name of the queue.
	 */
	@Nullable
	String declareQueue(Queue queue);

	/**
	 * Delete a queue, without regard for whether it is in use or has messages on it.
	 * @param queueName the name of the queue.
	 * @return true if the queue existed and was deleted.
	 */
	boolean deleteQueue(String queueName);

	// Note that nowait option is not readily exposed in Rabbit Java API but is for Rabbit .NET API.

	/**
	 * Delete a queue.
	 * @param queueName the name of the queue.
	 * @param unused true if the queue should be deleted only if not in use.
	 * @param empty true if the queue should be deleted only if empty.
	 */
	void deleteQueue(String queueName, boolean unused, boolean empty);

	/**
	 * Purges the contents of the given queue.
	 * @param queueName the name of the queue.
	 * @param noWait true to not await completion of the purge.
	 */
	void purgeQueue(String queueName, boolean noWait);

	/**
	 * Purges the contents of the given queue.
	 * @param queueName the name of the queue.
	 * @return the number of messages purged.
	 * @since 2.1
	 */
	int purgeQueue(String queueName);

	// Binding operations

	/**
	 * Declare a binding of a queue to an exchange.
	 * @param binding a description of the binding to declare.
	 */
	void declareBinding(Binding binding);

	/**
	 * Remove a binding of a queue to an exchange. Note unbindQueue/removeBinding was not introduced until 0.9 of the
	 * specification.
	 * @param binding a description of the binding to remove.
	 */
	void removeBinding(Binding binding);

	/**
	 * Returns an implementation-specific Map of properties if the queue exists.
	 * @param queueName the name of the queue.
	 * @return the properties or null if the queue doesn't exist.
	 */
	@Nullable
	Properties getQueueProperties(String queueName);

	/**
	 * Returns information about the queue, if it exists.
	 * @param queueName the name of the queue.
	 * @return the information or null if the queue doesn't exist.
	 */
	@Nullable
	QueueInformation getQueueInfo(String queueName);

	/**
	 * Return the manually declared AMQP objects.
	 * @return the manually declared AMQP objects.
	 * @since 2.4.13
	 * @deprecated in favor of {@link #getManualDeclarableSet()}.
	 */
	@Deprecated
	default Map<String, Declarable> getManualDeclarables() {
		return Collections.emptyMap();
	}

	/**
	 * Return the manually declared AMQP objects.
	 * @return the manually declared AMQP objects.
	 * @since 2.4.15
	 */
	default Set<Declarable> getManualDeclarableSet() {
		return Collections.emptySet();
	}

	/**
	 * Initialize the admin.
	 * @since 2.1
	 */
	default void initialize() {
		// no op
	}

}
