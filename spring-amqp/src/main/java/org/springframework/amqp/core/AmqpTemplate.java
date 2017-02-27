/*
 * Copyright 2002-2016 the original author or authors.
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

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.core.ParameterizedTypeReference;

/**
 * Specifies a basic set of AMQP operations.
 *
 * Provides synchronous send and receive methods. The {@link #convertAndSend(Object)} and
 * {@link #receiveAndConvert()} methods allow let you send and receive POJO objects.
 * Implementations are expected to delegate to an instance of {@link MessageConverter} to
 * perform conversion to and from AMQP byte[] payload type.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Artem Bilan
 * @author Ernest Sadykov
 * @author Gary Russell
 */
public interface AmqpTemplate {

	// send methods for messages

	/**
	 * Send a message to a default exchange with a default routing key.
	 *
	 * @param message a message to send
	 * @throws AmqpException if there is a problem
	 */
	void send(Message message) throws AmqpException;

	/**
	 * Send a message to a default exchange with a specific routing key.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @throws AmqpException if there is a problem
	 */
	void send(String routingKey, Message message) throws AmqpException;

	/**
	 * Send a message to a specific exchange with a specific routing key.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @throws AmqpException if there is a problem
	 */
	void send(String exchange, String routingKey, Message message) throws AmqpException;

	// send methods with conversion

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a default exchange
	 * with a default routing key.
	 *
	 * @param message a message to send
	 * @throws AmqpException if there is a problem
	 */
	void convertAndSend(Object message) throws AmqpException;

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a default exchange
	 * with a specific routing key.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @throws AmqpException if there is a problem
	 */
	void convertAndSend(String routingKey, Object message) throws AmqpException;

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a specific exchange
	 * with a specific routing key.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @throws AmqpException if there is a problem
	 */
	void convertAndSend(String exchange, String routingKey, Object message) throws AmqpException;

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a default exchange
	 * with a default routing key.
	 *
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @throws AmqpException if there is a problem
	 */
	void convertAndSend(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException;

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a default exchange
	 * with a specific routing key.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @throws AmqpException if there is a problem
	 */
	void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor)
			throws AmqpException;

	/**
	 * Convert a Java object to an Amqp {@link Message} and send it to a specific exchange
	 * with a specific routing key.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @throws AmqpException if there is a problem
	 */
	void convertAndSend(String exchange, String routingKey, Object message, MessagePostProcessor messagePostProcessor)
			throws AmqpException;

	// receive methods for messages

	/**
	 * Receive a message if there is one from a default queue. Returns immediately,
	 * possibly with a null value.
	 *
	 * @return a message or null if there is none waiting
	 * @throws AmqpException if there is a problem
	 */
	Message receive() throws AmqpException;

	/**
	 * Receive a message if there is one from a specific queue. Returns immediately,
	 * possibly with a null value.
	 *
	 * @param queueName the name of the queue to poll
	 * @return a message or null if there is none waiting
	 * @throws AmqpException if there is a problem
	 */
	Message receive(String queueName) throws AmqpException;

	/**
	 * Receive a message from a default queue, waiting up to the specified wait time if
	 * necessary for a message to become available.
	 *
	 * @param timeoutMillis how long to wait before giving up. Zero value means the method
	 * will return {@code null} immediately if there is no message available. Negative
	 * value makes method wait for a message indefinitely.
	 * @return a message or null if the time expires
	 * @throws AmqpException if there is a problem
	 * @since 1.6
	 */
	Message receive(long timeoutMillis) throws AmqpException;

	/**
	 * Receive a message from a specific queue, waiting up to the specified wait time if
	 * necessary for a message to become available.
	 *
	 * @param queueName the queue to receive from
	 * @param timeoutMillis how long to wait before giving up. Zero value means the method
	 * will return {@code null} immediately if there is no message available. Negative
	 * value makes method wait for a message indefinitely.
	 * @return a message or null if the time expires
	 * @throws AmqpException if there is a problem
	 * @since 1.6
	 */
	Message receive(String queueName, long timeoutMillis) throws AmqpException;

	// receive methods with conversion

	/**
	 * Receive a message if there is one from a default queue and convert it to a Java
	 * object. Returns immediately, possibly with a null value.
	 *
	 * @return a message or null if there is none waiting
	 * @throws AmqpException if there is a problem
	 */
	Object receiveAndConvert() throws AmqpException;

	/**
	 * Receive a message if there is one from a specific queue and convert it to a Java
	 * object. Returns immediately, possibly with a null value.
	 *
	 * @param queueName the name of the queue to poll
	 * @return a message or null if there is none waiting
	 * @throws AmqpException if there is a problem
	 */
	Object receiveAndConvert(String queueName) throws AmqpException;

	/**
	 * Receive a message if there is one from a default queue and convert it to a Java
	 * object. Wait up to the specified wait time if necessary for a message to become
	 * available.
	 *
	 * @param timeoutMillis how long to wait before giving up. Zero value means the method
	 * will return {@code null} immediately if there is no message available. Negative
	 * value makes method wait for a message indefinitely.
	 * @return a message or null if the time expires
	 * @throws AmqpException if there is a problem
	 * @since 1.6
	 */
	Object receiveAndConvert(long timeoutMillis) throws AmqpException;

	/**
	 * Receive a message if there is one from a specific queue and convert it to a Java
	 * object. Wait up to the specified wait time if necessary for a message to become
	 * available.
	 *
	 * @param queueName the name of the queue to poll
	 * @param timeoutMillis how long to wait before giving up. Zero value means the method
	 * will return {@code null} immediately if there is no message available. Negative
	 * value makes method wait for a message indefinitely.
	 * @return a message or null if the time expires
	 * @throws AmqpException if there is a problem
	 * @since 1.6
	 */
	Object receiveAndConvert(String queueName, long timeoutMillis) throws AmqpException;

	/**
	 * Receive a message if there is one from a default queue and convert it to a Java
	 * object. Returns immediately, possibly with a null value. Requires a
	 * {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 *
	 * @param type the type to convert to.
	 * @param <T> the type.
	 * @return a message or null if there is none waiting.
	 * @throws AmqpException if there is a problem.
	 * @since 2.0
	 */
	<T> T receiveAndConvert(ParameterizedTypeReference<T> type) throws AmqpException;

	/**
	 * Receive a message if there is one from a specific queue and convert it to a Java
	 * object. Returns immediately, possibly with a null value. Requires a
	 * {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 *
	 * @param queueName the name of the queue to poll
	 * @param type the type to convert to.
	 * @param <T> the type.
	 * @return a message or null if there is none waiting
	 * @throws AmqpException if there is a problem
	 * @since 2.0
	 */
	<T> T receiveAndConvert(String queueName, ParameterizedTypeReference<T> type) throws AmqpException;

	/**
	 * Receive a message if there is one from a default queue and convert it to a Java
	 * object. Wait up to the specified wait time if necessary for a message to become
	 * available. Requires a
	 * {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 *
	 * @param timeoutMillis how long to wait before giving up. Zero value means the method
	 * will return {@code null} immediately if there is no message available. Negative
	 * value makes method wait for a message indefinitely.
	 * @param type the type to convert to.
	 * @param <T> the type.
	 * @return a message or null if the time expires
	 * @throws AmqpException if there is a problem
	 * @since 2.0
	 */
	<T> T receiveAndConvert(long timeoutMillis, ParameterizedTypeReference<T> type) throws AmqpException;

	/**
	 * Receive a message if there is one from a specific queue and convert it to a Java
	 * object. Wait up to the specified wait time if necessary for a message to become
	 * available. Requires a
	 * {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 *
	 * @param queueName the name of the queue to poll
	 * @param timeoutMillis how long to wait before giving up. Zero value means the method
	 * will return {@code null} immediately if there is no message available. Negative
	 * value makes method wait for a message indefinitely.
	 * @param type the type to convert to.
	 * @param <T> the type.
	 * @return a message or null if the time expires
	 * @throws AmqpException if there is a problem
	 * @since 2.0
	 */
	<T> T receiveAndConvert(String queueName, long timeoutMillis, ParameterizedTypeReference<T> type)
			throws AmqpException;

	// receive and send methods for provided callback

	/**
	 * Receive a message if there is one from a default queue, invoke provided
	 * {@link ReceiveAndReplyCallback} and send reply message, if the {@code callback}
	 * returns one, to the {@code replyTo} {@link org.springframework.amqp.core.Address}
	 * from {@link org.springframework.amqp.core.MessageProperties} or to default exchange
	 * and default routingKey.
	 *
	 * @param callback a user-provided {@link ReceiveAndReplyCallback} implementation to
	 * process received message and return a reply message.
	 * @param <R> The type of the request after conversion from the {@link Message}.
	 * @param <S> The type of the response.
	 * @return {@code true}, if message was received
	 * @throws AmqpException if there is a problem
	 */
	<R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback) throws AmqpException;

	/**
	 * Receive a message if there is one from provided queue, invoke provided
	 * {@link ReceiveAndReplyCallback} and send reply message, if the {@code callback}
	 * returns one, to the {@code replyTo} {@link org.springframework.amqp.core.Address}
	 * from {@link org.springframework.amqp.core.MessageProperties} or to default exchange
	 * and default routingKey.
	 *
	 * @param queueName the queue name to receive a message.
	 * @param callback a user-provided {@link ReceiveAndReplyCallback} implementation to
	 * process received message and return a reply message.
	 * @param <R> The type of the request after conversion from the {@link Message}.
	 * @param <S> The type of the response.
	 * @return {@code true}, if message was received.
	 * @throws AmqpException if there is a problem.
	 */
	<R, S> boolean receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback) throws AmqpException;

	/**
	 * Receive a message if there is one from default queue, invoke provided
	 * {@link ReceiveAndReplyCallback} and send reply message, if the {@code callback}
	 * returns one, to the provided {@code exchange} and {@code routingKey}.
	 *
	 * @param callback a user-provided {@link ReceiveAndReplyCallback} implementation to
	 * process received message and return a reply message.
	 * @param replyExchange the exchange name to send reply message.
	 * @param replyRoutingKey the routing key to send reply message.
	 * @param <R> The type of the request after conversion from the {@link Message}.
	 * @param <S> The type of the response.
	 * @return {@code true}, if message was received.
	 * @throws AmqpException if there is a problem.
	 */
	<R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback, String replyExchange, String replyRoutingKey)
			throws AmqpException;

	/**
	 * Receive a message if there is one from provided queue, invoke provided
	 * {@link ReceiveAndReplyCallback} and send reply message, if the {@code callback}
	 * returns one, to the provided {@code exchange} and {@code routingKey}.
	 *
	 *
	 * @param queueName the queue name to receive a message.
	 * @param callback a user-provided {@link ReceiveAndReplyCallback} implementation to
	 * process received message and return a reply message.
	 * @param replyExchange the exchange name to send reply message.
	 * @param replyRoutingKey the routing key to send reply message.
	 * @param <R> The type of the request after conversion from the {@link Message}.
	 * @param <S> The type of the response.
	 * @return {@code true}, if message was received
	 * @throws AmqpException if there is a problem
	 */
	<R, S> boolean receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback, String replyExchange,
			String replyRoutingKey) throws AmqpException;

	/**
	 * Receive a message if there is one from a default queue, invoke provided
	 * {@link ReceiveAndReplyCallback} and send reply message, if the {@code callback}
	 * returns one, to the {@code replyTo} {@link org.springframework.amqp.core.Address}
	 * from result of {@link ReplyToAddressCallback}.
	 *
	 * @param callback a user-provided {@link ReceiveAndReplyCallback} implementation to
	 * process received message and return a reply message.
	 * @param replyToAddressCallback the callback to determine replyTo address at runtime.
	 * @param <R> The type of the request after conversion from the {@link Message}.
	 * @param <S> The type of the response.
	 * @return {@code true}, if message was received.
	 * @throws AmqpException if there is a problem.
	 */
	<R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback,
			ReplyToAddressCallback<S> replyToAddressCallback) throws AmqpException;

	/**
	 * Receive a message if there is one from provided queue, invoke provided
	 * {@link ReceiveAndReplyCallback} and send reply message, if the {@code callback}
	 * returns one, to the {@code replyTo} {@link org.springframework.amqp.core.Address}
	 * from result of {@link ReplyToAddressCallback}.
	 *
	 * @param queueName the queue name to receive a message.
	 * @param callback a user-provided {@link ReceiveAndReplyCallback} implementation to
	 * process received message and return a reply message.
	 * @param replyToAddressCallback the callback to determine replyTo address at runtime.
	 * @param <R> The type of the request after conversion from the {@link Message}.
	 * @param <S> The type of the response.
	 * @return {@code true}, if message was received
	 * @throws AmqpException if there is a problem
	 */
	<R, S> boolean receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback,
			ReplyToAddressCallback<S> replyToAddressCallback) throws AmqpException;

	// send and receive methods for messages

	/**
	 * Basic RPC pattern. Send a message to a default exchange with a default routing key
	 * and attempt to receive a response. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param message a message to send
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Message sendAndReceive(Message message) throws AmqpException;

	/**
	 * Basic RPC pattern. Send a message to a default exchange with a specific routing key
	 * and attempt to receive a response. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Message sendAndReceive(String routingKey, Message message) throws AmqpException;

	/**
	 * Basic RPC pattern. Send a message to a specific exchange with a specific routing
	 * key and attempt to receive a response. Implementations will normally set the
	 * reply-to header to an exclusive queue and wait up for some time limited by a
	 * timeout.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Message sendAndReceive(String exchange, String routingKey, Message message) throws AmqpException;

	// send and receive methods with conversion

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a default routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param message a message to send
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(Object message) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(String routingKey, Object message) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * specific exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(String exchange, String routingKey, Object message) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a default routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 *
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(String routingKey, Object message, MessagePostProcessor messagePostProcessor)
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
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 */
	Object convertSendAndReceive(String exchange, String routingKey, Object message,
			MessagePostProcessor messagePostProcessor) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a default routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a
	 * {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 * @param message a message to send.
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one.
	 * @throws AmqpException if there is a problem.
	 * @since 2.0
	 */
	<T> T convertSendAndReceiveAsType(Object message, ParameterizedTypeReference<T> responseType)
			throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 * @since 2.0
	 */
	<T> T convertSendAndReceiveAsType(String routingKey, Object message,
			ParameterizedTypeReference<T> responseType) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * specific exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 * @since 2.0
	 */
	<T> T convertSendAndReceiveAsType(String exchange, String routingKey, Object message,
			ParameterizedTypeReference<T> responseType) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a default routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 * @since 2.0
	 */
	<T> T convertSendAndReceiveAsType(Object message, MessagePostProcessor messagePostProcessor,
			ParameterizedTypeReference<T> responseType) throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * default exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 * @since 2.0
	 */
	<T> T convertSendAndReceiveAsType(String routingKey, Object message,
			MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<T> responseType)
			throws AmqpException;

	/**
	 * Basic RPC pattern with conversion. Send a Java object converted to a message to a
	 * specific exchange with a specific routing key and attempt to receive a response,
	 * converting that to a Java object. Implementations will normally set the reply-to
	 * header to an exclusive queue and wait up for some time limited by a timeout.
	 * Requires a {@link org.springframework.amqp.support.converter.SmartMessageConverter}.
	 * @param exchange the name of the exchange
	 * @param routingKey the routing key
	 * @param message a message to send
	 * @param messagePostProcessor a processor to apply to the message before it is sent
	 * @param responseType the type to convert the reply to.
	 * @param <T> the type.
	 * @return the response if there is one
	 * @throws AmqpException if there is a problem
	 * @since 2.0
	 */
	<T> T convertSendAndReceiveAsType(String exchange, String routingKey, Object message,
			MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<T> responseType)
			throws AmqpException;

}
