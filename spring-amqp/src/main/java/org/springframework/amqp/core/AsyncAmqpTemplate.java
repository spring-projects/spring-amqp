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

package org.springframework.amqp.core;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * Classes implementing this interface can perform asynchronous send and
 * receive operations.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public interface AsyncAmqpTemplate {

	/**
	 * Send a message to the default exchange with the default routing key. If the message
	 * contains a correlationId property, it must be unique.
	 * @param message the message.
	 * @return the {@link ListenableFuture}.
	 */
	ListenableFuture<Message> sendAndReceive(Message message);

	/**
	 * Send a message to the default exchange with the supplied routing key. If the message
	 * contains a correlationId property, it must be unique.
	 * @param routingKey the routing key.
	 * @param message the message.
	 * @return the {@link ListenableFuture}.
	 */
	ListenableFuture<Message> sendAndReceive(String routingKey, Message message);

	/**
	 * Send a message to the supplied exchange and routing key. If the message
	 * contains a correlationId property, it must be unique.
	 * @param exchange the exchange.
	 * @param routingKey the routing key.
	 * @param message the message.
	 * @return the {@link ListenableFuture}.
	 */
	ListenableFuture<Message> sendAndReceive(String exchange, String routingKey, Message message);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * default routing key.
	 * @param object the object to convert.
	 * @param <C> the expected result type.
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceive(Object object);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * provided routing key.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param <C> the expected result type.
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceive(String routingKey, Object object);

	/**
	 * Convert the object to a message and send it to the provided exchange and
	 * routing key.
	 * @param exchange the exchange.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param <C> the expected result type.
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceive(String exchange, String routingKey, Object object);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * default routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param object the object to convert.
	 * @param messagePostProcessor the post processor.
	 * @param <C> the expected result type.
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceive(Object object, MessagePostProcessor messagePostProcessor);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * provided routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param messagePostProcessor the post processor.
	 * @param <C> the expected result type.
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceive(String routingKey, Object object,
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
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceive(String exchange, String routingKey, Object object,
			MessagePostProcessor messagePostProcessor);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * default routing key.
	 * @param object the object to convert.
	 * @param responseType the response type.
	 * @param <C> the expected result type.
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceiveAsType(Object object, ParameterizedTypeReference<C> responseType);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * provided routing key.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param responseType the response type.
	 * @param <C> the expected result type.
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceiveAsType(String routingKey, Object object,
			ParameterizedTypeReference<C> responseType);

	/**
	 * Convert the object to a message and send it to the provided exchange and
	 * routing key.
	 * @param exchange the exchange.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param responseType the response type.
	 * @param <C> the expected result type.
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceiveAsType(String exchange, String routingKey, Object object,
			ParameterizedTypeReference<C> responseType);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * default routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param object the object to convert.
	 * @param messagePostProcessor the post processor.
	 * @param responseType the response type.
	 * @param <C> the expected result type.
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceiveAsType(Object object, MessagePostProcessor messagePostProcessor,
			ParameterizedTypeReference<C> responseType);

	/**
	 * Convert the object to a message and send it to the default exchange with the
	 * provided routing key after invoking the {@link MessagePostProcessor}.
	 * If the post processor adds a correlationId property, it must be unique.
	 * @param routingKey the routing key.
	 * @param object the object to convert.
	 * @param messagePostProcessor the post processor.
	 * @param responseType the response type.
	 * @param <C> the expected result type.
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceiveAsType(String routingKey, Object object,
			MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<C> responseType);

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
	 * @return the {@link ListenableFuture}.
	 */
	<C> ListenableFuture<C> convertSendAndReceiveAsType(String exchange, String routingKey, Object object,
			MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<C> responseType);

}
