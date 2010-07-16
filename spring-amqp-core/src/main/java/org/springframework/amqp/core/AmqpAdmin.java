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

package org.springframework.amqp.core;

/**
 * Specifies a basic set of portable AMQP administrative operations for AMQP > 0.8
 * 
 * @author Mark Pollack
 */
public interface AmqpAdmin {

	/**
	 * Declare an exchange
	 * @param exchange the exchange to declare.
	 */
	void declareExchange(Exchange exchange);

	/**
	 * Delete an exchange.  Look at implementation specific subclass for implementation specific behavior, for example
	 * for RabbitMQ this will delete the exchange without regard for whether it is in use or not.
	 * @param exchangeName the name of the exchange
	 */
	void deleteExchange(String exchangeName);


	// Queue Operations
	
	/**
	 * Declare a queue whose name is automatically named.  It is created with 
	 * exclusive = true, autoDelete=true, and durable = false.
	 */
	Queue declareQueue();

	/**
	 * Declare the given queue
	 * @param queue the queue to declare
	 */
	void declareQueue(Queue queue);

	/**
	 * Delete a queue, without regard for whether it is in use or has messages on it 
	 * @param queueName the name of the queue
	 */
	void deleteQueue(String queueName);

	// Note that nowait option is not readily exposed in Rabbit Java API but is for Rabbit .NET API. 

	/**
	 * Delete a queue
	 * @param queueName the name of the queue
	 * @param unused true if the queue should be deleted only if not in use
	 * @param empty true if the queue should be deleted only if empty 
	 */
	void deleteQueue(String queueName, boolean unused, boolean empty);

	/**
	 * Purges the contents of the given queue. 
	 * @param queueName the name of the queue
	 * @param noWait true to not await completion of the purge
	 */
	void purgeQueue(String queueName, boolean noWait);


	// Binding opertaions

	/**
	 * Declare a binding of a queue to an exchange.
	 * @param binding a description of the binding to declare.
	 */
	void declareBinding(Binding binding);


	//Note unbindQueue/removeBinding was not introduced until 0.9 of the specification.

}
