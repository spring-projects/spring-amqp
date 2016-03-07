/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.amqp.core;

import java.util.List;

/**
 * Interface specifying management operations.
 *
 * @author Gary Russell
 * @since 1.5
 *
 */
public interface AmqpManagementOperations {

	/**
	 * Add an exchange to the default vhost ('/').
	 * @param exchange the exchange.
	 */
	void addExchange(Exchange exchange);

	/**
	 * Add an exchange to the specified vhost.
	 * @param vhost the vhost.
	 * @param exchange the exchange.
	 */
	void addExchange(String vhost, Exchange exchange);

	/**
	 * Purge a queue in the default vhost ('/').
	 * @param queue the queue.
	 */
	void purgeQueue(Queue queue);

	/**
	 * Purge a queue in the provided vhost.
	 * @param vhost the vhost.
	 * @param queue the queue.
	 */
	void purgeQueue(String vhost, Queue queue);

	/**
	 * Delete a queue from the default vhost ('/').
	 * @param queue the queue.
	 */
	void deleteQueue(Queue queue);

	/**
	 * Delete a queue from the provided vhost.
	 * @param vhost the vhost.
	 * @param queue the queue.
	 */
	void deleteQueue(String vhost, Queue queue);

	/**
	 * Get a specific queue from the default vhost ('/').
	 * @param name the queue name.
	 * @return the Queue.
	 */
	Queue getQueue(String name);

	/**
	 * Get a specific queue from the provided vhost.
	 * @param vhost the vhost.
	 * @param name the queue name.
	 * @return the Queue.
	 */
	Queue getQueue(String vhost, String name);

	/**
	 * Get all queues.
	 * @return the queues.
	 */
	List<Queue> getQueues();

	/**
	 * Get all queues in the provided vhost.
	 * @param vhost the vhost.
	 * @return the queues.
	 */
	List<Queue> getQueues(String vhost);

	/**
	 * Add a queue to the default vhost ('/').
	 * @param queue the queue.
	 */
	void addQueue(Queue queue);

	/**
	 * Add a queue to the specified vhost.
	 * @param vhost the vhost.
	 * @param queue the queue.
	 */
	void addQueue(String vhost, Queue queue);

	/**
	 * Delete an exchange from the default vhost ('/').
	 * @param exchange the queue.
	 */
	void deleteExchange(Exchange exchange);

	/**
	 * Delete an exchange from the provided vhost.
	 * @param vhost the vhost.
	 * @param exchange the queue.
	 */
	void deleteExchange(String vhost, Exchange exchange);

	/**
	 * Get a specific queue from the default vhost ('/').
	 * @param name the exchange name.
	 * @return the Exchange.
	 */
	Exchange getExchange(String name);

	/**
	 * Get a specific exchange from the provided vhost.
	 * @param vhost the vhost.
	 * @param name the exchange name.
	 * @return the Exchange.
	 */
	Exchange getExchange(String vhost, String name);

	/**
	 * Get all exchanges.
	 * @return the exchanges.
	 */
	List<Exchange> getExchanges();

	/**
	 * Get all exchanges in the provided vhost. Only {@link DirectExchange},
	 * {@link FanoutExchange}, {@link HeadersExchange} and {@link TopicExchange}s
	 * are returned.
	 * @param vhost the vhost.
	 * @return the exchanges.
	 */
	List<Exchange> getExchanges(String vhost);

	/**
	 * Get all bindings.
	 * @return the bindings.
	 */
	List<Binding> getBindings();

	/**
	 * Get all bindings in the provided vhost.
	 * @param vhost the vhost.
	 * @return the bindings.
	 */
	List<Binding> getBindings(String vhost);

	/**
	 * Get all bindings from the provided exchange in the provided vhost.
	 * @param vhost the vhost.
	 * @param exchange the exchange name.
	 * @return the bindings.
	 */
	List<Binding> getBindingsForExchange(String vhost, String exchange);

}
