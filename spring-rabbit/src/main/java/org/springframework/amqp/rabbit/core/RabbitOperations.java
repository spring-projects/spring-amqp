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

package org.springframework.amqp.rabbit.core;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.core.ParameterizedTypeReference;

/**
 * Rabbit specific methods for Amqp functionality.
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Gary Russell
 */
public interface RabbitOperations extends AmqpTemplate {

	/**
	 * Execute the callback with a channel and reliably close the channel afterwards.
	 * @param action the call back.
	 * @param <T> the return type.
	 * @return the result from the
	 * {@link ChannelCallback#doInRabbit(com.rabbitmq.client.Channel)}.
	 * @throws AmqpException if one occurs.
	 */
	<T> T execute(ChannelCallback<T> action) throws AmqpException;

	/**
	 * Invoke the callback and run all operations on the template argument in a dedicated
	 * thread-bound channel and reliably close the channel afterwards.
	 * @param action the call back.
	 * @param <T> the return type.
	 * @return the result from the
	 * {@link OperationsCallback#doInRabbit(RabbitOperations operations)}.
	 * @throws AmqpException if one occurs.
	 * @since 2.0
	 */
	<T> T invoke(OperationsCallback<T> action) throws AmqpException;

	/**
	 * Delegate to the underlying dedicated channel to wait for confirms. The connection
	 * factory must be configured for publisher confirms and this method must be called
	 * within the scope of an {@link #invoke(OperationsCallback)} operation.
	 * Requires {@code CachingConnectionFactory#setPublisherConfirms(true)}.
	 * @param timeout the timeout
	 * @return true if acks and no nacks are received.
	 * @throws AmqpException if one occurs.
	 * @since 2.0
	 * @see com.rabbitmq.client.Channel#waitForConfirms(long)
	 */
	boolean waitForConfirms(long timeout) throws AmqpException;

	/**
	 * Delegate to the underlying dedicated channel to wait for confirms. The connection
	 * factory must be configured for publisher confirms and this method must be called
	 * within the scope of an {@link #invoke(OperationsCallback)} operation.
	 * Requires {@code CachingConnectionFactory#setPublisherConfirms(true)}.
	 * @param timeout the timeout
	 * @throws AmqpException if one occurs.
	 * @since 2.0
	 * @see com.rabbitmq.client.Channel#waitForConfirmsOrDie(long)
	 */
	void waitForConfirmsOrDie(long timeout) throws AmqpException;

	/**
	 * Return the connection factory for this operations.
	 * @return the connection factory.
	 * @since 2.0
	 */
	ConnectionFactory getConnectionFactory();

	/**
	 * Send a message to a specific exchange with a specific routing key.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param correlationData data to correlate publisher confirms.
	 * @throws AmqpException if there is a problem
	 */
	void send(String exchange, String routingKey, Message message, CorrelationData correlationData)
			throws AmqpException;

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a default exchange
	 * with a default routing key.
	 *
	 * @param message a message to send
	 * @param correlationData data to correlate publisher confirms.
	 * @throws AmqpException if there is a problem
	 */
	void correlationConvertAndSend(Object message, CorrelationData correlationData) throws AmqpException;

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a default exchange
	 * with a specific routing key.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param correlationData data to correlate publisher confirms.
	 * @throws AmqpException if there is a problem
	 */
	void convertAndSend(String routingKey, Object message, CorrelationData correlationData) throws AmqpException;

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a specific exchange
	 * with a specific routing key.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param correlationData data to correlate publisher confirms.
	 * @throws AmqpException if there is a problem
	 */
	void convertAndSend(String exchange, String routingKey, Object message, CorrelationData correlationData)
			throws AmqpException;

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a default exchange
	 * with a default routing key.
	 *
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param correlationData data to correlate publisher confirms.
	 * @throws AmqpException if there is a problem
	 */
	void convertAndSend(Object message, MessagePostProcessor messagePostProcessor, CorrelationData correlationData)
			throws AmqpException;

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a default exchange
	 * with a specific routing key.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param correlationData data to correlate publisher confirms.
	 * @throws AmqpException if there is a problem
	 */
	void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor,
			CorrelationData correlationData) throws AmqpException;

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a specific exchange
	 * with a specific routing key.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param correlationData data to correlate publisher confirms.
	 * @throws AmqpException if there is a problem
	 */
	void convertAndSend(String exchange, String routingKey, Object message, MessagePostProcessor messagePostProcessor,
			CorrelationData correlationData) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a default routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param message a message to send.
	 * @param correlationData data to correlate publisher confirms.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(Object message, CorrelationData correlationData) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param correlationData data to correlate publisher confirms.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(String routingKey, Object message, CorrelationData correlationData)
			throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * specific exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param correlationData data to correlate publisher confirms.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(String exchange, String routingKey, Object message,
			CorrelationData correlationData) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a default routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param correlationData data to correlate publisher confirms.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(Object message, MessagePostProcessor messagePostProcessor,
			CorrelationData correlationData) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param correlationData data to correlate publisher confirms.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(String routingKey, Object message,
			MessagePostProcessor messagePostProcessor, CorrelationData correlationData) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * specific exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param correlationData data to correlate publisher confirms.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(String exchange, String routingKey, Object message,
			MessagePostProcessor messagePostProcessor, CorrelationData correlationData)
			throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a default routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a
	 * {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 *
	 * @param message a message to send.
	 * @param correlationData data to correlate publisher confirms.
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one.
	 * @throws AmqpException if there is a problem.
	 */
	<T> T convertSendAndReceiveAsType(Object message, CorrelationData correlationData,
			ParameterizedTypeReference<T> responseType) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a
	 * {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param correlationData data to correlate publisher confirms.
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	<T> T convertSendAndReceiveAsType(String routingKey, Object message, CorrelationData correlationData,
			ParameterizedTypeReference<T> responseType) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * specific exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a
	 * {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param correlationData data to correlate publisher confirms.
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	<T> T convertSendAndReceiveAsType(String exchange, String routingKey, Object message,
			CorrelationData correlationData, ParameterizedTypeReference<T> responseType) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a default routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a
	 * {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 *
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param correlationData data to correlate publisher confirms.
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	<T> T convertSendAndReceiveAsType(Object message, MessagePostProcessor messagePostProcessor,
			CorrelationData correlationData, ParameterizedTypeReference<T> responseType) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a
	 * {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param correlationData data to correlate publisher confirms.
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	<T> T convertSendAndReceiveAsType(String routingKey, Object message,
			MessagePostProcessor messagePostProcessor, CorrelationData correlationData,
			ParameterizedTypeReference<T> responseType) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * specific exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a
	 * {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param correlationData data to correlate publisher confirms.
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	<T> T convertSendAndReceiveAsType(String exchange, String routingKey, Object message,
			MessagePostProcessor messagePostProcessor, CorrelationData correlationData,
			ParameterizedTypeReference<T> responseType) throws AmqpException;

	/**
	 * Callback for using the same channel for multiple RabbitTemplate
	 * operations.
	 * @param <T> the type the callback returns.
	 *
	 * @since 2.0
	 */
	@FunctionalInterface
	public interface OperationsCallback<T> {

		/**
		 * Execute any number of operations using a dedicated
		 * {@link com.rabbitmq.client.Channel} as long as those operations are performed
		 * on the template argument and on the calling thread. The channel will be
		 * physically closed when the callback exits.
		 *
		 * @param operations The operations.
		 * @return The result.
		 */
		T doInRabbit(RabbitOperations operations);

	}

}
