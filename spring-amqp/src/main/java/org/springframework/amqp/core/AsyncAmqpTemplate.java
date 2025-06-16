/*
 * Copyright 2020-present the original author or authors.
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

import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;

/**
 * Classes implementing this interface can perform asynchronous send and
 * receive operations using {@link CompletableFuture}s.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.0
 *
 */
public interface AsyncAmqpTemplate {

	default CompletableFuture<Boolean> send(Message message) {
		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Boolean> send(String queue, Message message) {
		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Boolean> send(String exchange, @Nullable String routingKey, Message message) {
		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Boolean> convertAndSend(Object message) {
		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Boolean> convertAndSend(String queue, Object message) {
		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Boolean> convertAndSend(String exchange, @Nullable String routingKey, Object message) {
		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Boolean> convertAndSend(Object message,
			@Nullable MessagePostProcessor messagePostProcessor) {

		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Boolean> convertAndSend(String queue, Object message,
			@Nullable MessagePostProcessor messagePostProcessor) {

		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Boolean> convertAndSend(String exchange, @Nullable String routingKey, Object message,
			@Nullable MessagePostProcessor messagePostProcessor) {

		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Message> receive() {
		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Message> receive(String queueName) {
		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Object> receiveAndConvert() {
		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Object> receiveAndConvert(String queueName) {
		throw new UnsupportedOperationException();
	}

	default <T> CompletableFuture<T> receiveAndConvert(@Nullable ParameterizedTypeReference<T> type) {
		throw new UnsupportedOperationException();
	}

	default <T> CompletableFuture<T> receiveAndConvert(String queueName, @Nullable ParameterizedTypeReference<T> type) {
		throw new UnsupportedOperationException();
	}

	default <R, S> CompletableFuture<Boolean> receiveAndReply(ReceiveAndReplyCallback<R, S> callback) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Perform a server-side RPC functionality.
	 * The request message must have a {@code replyTo} property.
	 * The request {@code messageId} property is used for correlation.
	 * The callback might not produce a reply with the meaning nothing to answer.
	 * @param <R> the request body type.
	 * @param <S> the response body type
	 * @param queueName the queue to consume request.
	 * @param callback an application callback to handle request and produce reply.
	 * @return the completion status: true if no errors and reply has been produced.
	 */
	default <R, S> CompletableFuture<Boolean> receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Send a message to the default exchange with the default routing key. If the message
	 * contains a correlationId property, it must be unique.
	 * @param message the message.
	 * @return the {@link CompletableFuture}.
	 */
	CompletableFuture<Message> sendAndReceive(Message message);

	/**
	 * Send a message to the default exchange with the supplied routing key. If the message
	 * contains a correlationId property, it must be unique.
	 * @param routingKey the routing key.
	 * @param message the message.
	 * @return the {@link CompletableFuture}.
	 */
	CompletableFuture<Message> sendAndReceive(String routingKey, Message message);

	/**
	 * Send a message to the supplied exchange and routing key. If the message
	 * contains a correlationId property, it must be unique.
	 * @param exchange the exchange.
	 * @param routingKey the routing key.
	 * @param message the message.
	 * @return the {@link CompletableFuture}.
	 */
	CompletableFuture<Message> sendAndReceive(String exchange, String routingKey, Message message);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * default routing key.
	 * @param object the object to convert.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceive(Object object);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * provided routing key.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceive(String routingKey, Object object);

	/**
	 * Convert the object to a message and send it to the provided exchange and
	 * routing key.
	 * @param exchange the exchange.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceive(String exchange, String routingKey, Object object);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * default routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param object the object to convert.
	 * @param messagePostProcessor the post processor.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceive(Object object, MessagePostProcessor messagePostProcessor);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * provided routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param messagePostProcessor the post processor.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceive(String routingKey, Object object,
			MessagePostProcessor messagePostProcessor);

	/**
	 * Convert the object to a message and send it to the provided exchange and
	 * routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param exchange the exchange
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param messagePostProcessor the post processor.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceive(String exchange, String routingKey, Object object,
			@Nullable MessagePostProcessor messagePostProcessor);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * default routing key.
	 * @param object the object to convert.
	 * @param responseType the response type.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceiveAsType(Object object, ParameterizedTypeReference<C> responseType);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * provided routing key.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param responseType the response type.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceiveAsType(String routingKey, Object object,
			ParameterizedTypeReference<C> responseType);

	/**
	 * Convert the object to a message and send it to the provided exchange and
	 * routing key.
	 * @param exchange the exchange.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param responseType the response type.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceiveAsType(String exchange, String routingKey, Object object,
			ParameterizedTypeReference<C> responseType);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * default routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param object the object to convert.
	 * @param messagePostProcessor the post processor.
	 * @param responseType the response type.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceiveAsType(Object object,
			@Nullable MessagePostProcessor messagePostProcessor,
			@Nullable ParameterizedTypeReference<C> responseType);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * provided routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param messagePostProcessor the post processor.
	 * @param responseType the response type.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceiveAsType(String routingKey, Object object,
			@Nullable MessagePostProcessor messagePostProcessor, @Nullable ParameterizedTypeReference<C> responseType);

	/**
	 * Convert the object to a message and send it to the provided exchange and
	 * routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param exchange the exchange
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param messagePostProcessor the post processor.
	 * @param responseType the response type.
	 * @param <C> the expected result type.
	 * @return the {@link CompletableFuture}.
	 */
	<C> CompletableFuture<C> convertSendAndReceiveAsType(String exchange, String routingKey, Object object,
			@Nullable MessagePostProcessor messagePostProcessor, @Nullable ParameterizedTypeReference<C> responseType);

}
