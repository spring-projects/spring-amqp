/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.rabbit.stream.producer;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.rabbit.stream.support.converter.StreamMessageConverter;
import org.springframework.util.concurrent.ListenableFuture;

import com.rabbitmq.stream.MessageBuilder;

/**
 * Provides methods for sending messages using a RabbitMQ Stream producer.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public interface RabbitStreamOperations extends AutoCloseable {

	/**
	 * Send a Spring AMQP message.
	 * @param message the message.
	 * @return a future to indicate success/failure.
	 */
	ListenableFuture<Boolean> send(Message message);

	/**
	 * Convert to and send a Spring AMQP message.
	 * @param message the payload.
	 * @return a future to indicate success/failure.
	 */
	ListenableFuture<Boolean> convertAndSend(Object message);

	/**
	 * Convert to and send a Spring AMQP message. If a {@link MessagePostProcessor} is
	 * provided and returns {@code null}, the message is not sent and the future is
	 * completed with {@code false}.
	 * @param message the payload.
	 * @param mpp a message post processor.
	 * @return a future to indicate success/failure.
	 */
	ListenableFuture<Boolean> convertAndSend(Object message, @Nullable MessagePostProcessor mpp);

	/**
	 * Send a native stream message.
	 * @param message the message.
	 * @return a future to indicate success/failure.
	 * @see #messageBuilder()
	 */
	ListenableFuture<Boolean> send(com.rabbitmq.stream.Message message);

	/**
	 * Return the producer's {@link MessageBuilder} to create native stream messages.
	 * @return the builder.
	 * @see #send(com.rabbitmq.stream.Message)
	 */
	MessageBuilder messageBuilder();

	/**
	 * Return the message converter.
	 * @return the converter.
	 */
	MessageConverter messageConverter();

	/**
	 * Return the stream message converter.
	 * @return the converter;
	 */
	StreamMessageConverter streamMessageConverter();

	@Override
	default void close() throws AmqpException {
		// narrow exception to avoid compiler warning - see
		// https://bugs.openjdk.java.net/browse/JDK-8155591
	}

}
