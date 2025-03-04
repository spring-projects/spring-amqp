/*
 * Copyright 2025 the original author or authors.
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

package org.springframework.amqp.rabbitmq.client;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.Resource;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.AsyncAmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReceiveAndReplyCallback;
import org.springframework.amqp.core.ReplyToAddressCallback;
import org.springframework.amqp.rabbit.core.AmqpNackReceivedException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.converter.SmartMessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.util.Assert;

/**
 * The {@link AmqpTemplate} for RabbitMQ AMQP 1.0 protocol support.
 * A Spring-friendly wrapper around {@link Environment#connectionBuilder()};
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public class RabbitAmqpTemplate implements AsyncAmqpTemplate, DisposableBean {

	private final AmqpConnectionFactory connectionFactory;

	private final Lock instanceLock = new ReentrantLock();

	private @Nullable Object publisher;

	private MessageConverter messageConverter = new SimpleMessageConverter();

	private @Nullable String defaultExchange;

	private @Nullable String defaultRoutingKey;

	private @Nullable String defaultQueue;

	private @Nullable String defaultReceiveQueue;

	private Resource.StateListener @Nullable [] stateListeners;

	private Duration publishTimeout = Duration.ofSeconds(60);

	public RabbitAmqpTemplate(AmqpConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public void setListeners(Resource.StateListener... listeners) {
		this.stateListeners = listeners;
	}

	public void setPublishTimeout(Duration timeout) {
		this.publishTimeout = timeout;
	}

	/**
	 * Set a default exchange for publishing.
	 * Cannot be real default AMQP exchange.
	 * The {@link #setQueue(String)} is recommended instead.
	 * Mutually exclusive with {@link #setQueue(String)}.
	 * @param exchange the default exchange
	 */
	public void setExchange(String exchange) {
		this.defaultExchange = exchange;
	}

	/**
	 * Set a default routing key.
	 * Mutually exclusive with {@link #setQueue(String)}.
	 * @param routingKey the default routing key.
	 */
	public void setRoutingKey(String routingKey) {
		this.defaultRoutingKey = routingKey;
	}

	/**
	 * Set default queue for publishing.
	 * Mutually exclusive with {@link #setExchange(String)} and {@link #setRoutingKey(String)}.
	 * @param queue the default queue.
	 */
	public void setQueue(String queue) {
		this.defaultQueue = queue;
	}

	/**
	 * Set a converter for {@link #convertAndSend(Object)} operations.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * The name of the default queue to receive messages from when none is specified explicitly.
	 * @param queue the default queue name to use for receive operation.
	 */
	public void setDefaultReceiveQueue(String queue) {
		this.defaultReceiveQueue = queue;
	}

	private String getRequiredQueue() throws IllegalStateException {
		String name = this.defaultReceiveQueue;
		Assert.state(name != null, "No 'queue' specified. Check configuration of this 'RabbitAmqpTemplate'.");
		return name;
	}

	private Publisher getPublisher() {
		Object publisherToReturn = this.publisher;
		if (publisherToReturn == null) {
			this.instanceLock.lock();
			try {
				publisherToReturn = this.publisher;
				if (publisherToReturn == null) {
					publisherToReturn =
							this.connectionFactory.getConnection()
									.publisherBuilder()
									.listeners(this.stateListeners)
									.publishTimeout(this.publishTimeout)
									.build();
					this.publisher = publisherToReturn;
				}
			}
			finally {
				this.instanceLock.unlock();
			}
		}
		return (Publisher) publisherToReturn;
	}

	@Override
	public void destroy() {
		Object publisherToClose = this.publisher;
		if (publisherToClose != null) {
			((Publisher) publisherToClose).close();
			this.publisher = null;
		}
	}

	/**
	 * Publish a message to the default exchange and routing key (if any) (or queue) configured on this template.
	 * @param message to publish
	 * @return the {@link CompletableFuture} as an async result of the message publication.
	 */
	@Override
	public CompletableFuture<Boolean> send(Message message) {
		Assert.state(this.defaultExchange != null || this.defaultQueue != null,
				"For send with defaults, an 'exchange' (and optional 'key') or 'queue' must be provided");
		return doSend(this.defaultExchange, this.defaultRoutingKey, this.defaultQueue, message);
	}

	/**
	 * Publish the message to the provided queue.
	 * @param queue to publish
	 * @param message to publish
	 * @return the {@link CompletableFuture} as an async result of the message publication.
	 */
	@Override
	public CompletableFuture<Boolean> send(String queue, Message message) {
		return doSend(null, null, queue, message);
	}

	@Override
	public CompletableFuture<Boolean> send(String exchange, @Nullable String routingKey, Message message) {
		return doSend(exchange, routingKey != null ? routingKey : this.defaultRoutingKey, null, message);
	}

	private CompletableFuture<Boolean> doSend(@Nullable String exchange, @Nullable String routingKey,
			@Nullable String queue, Message message) {

		com.rabbitmq.client.amqp.Message amqpMessage = getPublisher().message();
		com.rabbitmq.client.amqp.Message.MessageAddressBuilder address = amqpMessage.toAddress();
		JavaUtils.INSTANCE
				.acceptIfNotNull(exchange, address::exchange)
				.acceptIfNotNull(routingKey, address::key)
				.acceptIfNotNull(queue, address::queue);

		amqpMessage = address.message();

		RabbitAmqpUtils.toAmqpMessage(message, amqpMessage);

		CompletableFuture<Boolean> publishResult = new CompletableFuture<>();

		getPublisher().publish(amqpMessage,
				(context) -> {
					switch (context.status()) {
						case ACCEPTED -> publishResult.complete(true);
						case REJECTED, RELEASED -> publishResult.completeExceptionally(
								new AmqpNackReceivedException("The message was rejected", message));
					}
				});

		return publishResult;
	}

	/**
	 * Publish a message from converted body to the default exchange
	 * and routing key (if any) (or queue) configured on this template.
	 * @param message to publish
	 * @return the {@link CompletableFuture} as an async result of the message publication.
	 */
	@Override
	public CompletableFuture<Boolean> convertAndSend(Object message) {
		Assert.state(this.defaultExchange != null || this.defaultQueue != null,
				"For send with defaults, an 'exchange' (and optional 'key') or 'queue' must be provided");
		return doConvertAndSend(this.defaultExchange, this.defaultRoutingKey, this.defaultQueue, message, null);
	}

	@Override
	public CompletableFuture<Boolean> convertAndSend(String queue, Object message) {
		return doConvertAndSend(null, null, queue, message, null);
	}

	@Override
	public CompletableFuture<Boolean> convertAndSend(String exchange, @Nullable String routingKey, Object message) {
		return doConvertAndSend(exchange, routingKey != null ? routingKey : this.defaultRoutingKey, null, message, null);
	}

	@Override
	public CompletableFuture<Boolean> convertAndSend(Object message,
			@Nullable MessagePostProcessor messagePostProcessor) {

		return doConvertAndSend(null, null, null, message, messagePostProcessor);
	}

	@Override
	public CompletableFuture<Boolean> convertAndSend(String queue, Object message,
			@Nullable MessagePostProcessor messagePostProcessor) {

		return doConvertAndSend(null, null, queue, message, messagePostProcessor);
	}

	@Override
	public CompletableFuture<Boolean> convertAndSend(String exchange, @Nullable String routingKey, Object message,
			@Nullable MessagePostProcessor messagePostProcessor) {

		return doConvertAndSend(exchange, routingKey, null, message, messagePostProcessor);
	}

	private CompletableFuture<Boolean> doConvertAndSend(@Nullable String exchange, @Nullable String routingKey,
			@Nullable String queue, Object data, @Nullable MessagePostProcessor messagePostProcessor) {

		Message message =
				data instanceof Message
						? (Message) data
						: this.messageConverter.toMessage(data, new MessageProperties());
		if (messagePostProcessor != null) {
			message = messagePostProcessor.postProcessMessage(message);
		}
		return doSend(exchange, routingKey, queue, message);
	}

	@Override
	public CompletableFuture<Message> receive() {
		return receive(getRequiredQueue());
	}

	@SuppressWarnings("try")
	@Override
	public CompletableFuture<Message> receive(String queueName) {
		CompletableFuture<Message> messageFuture = new CompletableFuture<>();

		Consumer consumer =
				this.connectionFactory.getConnection()
						.consumerBuilder()
						.queue(queueName)
						.initialCredits(1)
						.priority(10)
						.messageHandler((context, message) -> {
							context.accept();
							messageFuture.complete(RabbitAmqpUtils.fromAmqpMessage(message, null));
						})
						.build();

		return messageFuture
				.orTimeout(1, TimeUnit.MINUTES)
				.whenComplete((message, exception) -> consumer.close());
	}

	@Override
	public CompletableFuture<Object> receiveAndConvert() {
		return receiveAndConvert(getRequiredQueue());
	}

	@Override
	public CompletableFuture<Object> receiveAndConvert(String queueName) {
		return receive(queueName)
				.thenApply(this.messageConverter::fromMessage);
	}

	/**
	 * Receive a message from {@link #setDefaultReceiveQueue(String)} and convert its body
	 * to the expected type.
	 * The {@link #setMessageConverter(MessageConverter)} must be an implementation of {@link SmartMessageConverter}.
	 * @param type the type to covert received result.
	 * @return the CompletableFuture with a result.
	 */
	@Override
	public <T> CompletableFuture<T> receiveAndConvert(ParameterizedTypeReference<T> type) {
		return receiveAndConvert(getRequiredQueue(), type);
	}

	/**
	 * Receive a message from {@link #setDefaultReceiveQueue(String)} and convert its body
	 * to the expected type.
	 * The {@link #setMessageConverter(MessageConverter)} must be an implementation of {@link SmartMessageConverter}.
	 * @param queueName the queue to consume message from.
	 * @param type the type to covert received result.
	 * @return the CompletableFuture with a result.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<T> receiveAndConvert(String queueName, ParameterizedTypeReference<T> type) {
		SmartMessageConverter smartMessageConverter = getRequiredSmartMessageConverter();
		return receive(queueName)
				.thenApply((message) -> (T) smartMessageConverter.fromMessage(message, type));
	}

	private SmartMessageConverter getRequiredSmartMessageConverter() throws IllegalStateException {
		Assert.state(this.messageConverter instanceof SmartMessageConverter,
				"template's message converter must be a SmartMessageConverter");
		return (SmartMessageConverter) this.messageConverter;
	}

	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	public <R, S> boolean receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback, String replyExchange, String replyRoutingKey) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	public <R, S> boolean receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback, String replyExchange, String replyRoutingKey) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback, ReplyToAddressCallback<S> replyToAddressCallback) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	public <R, S> boolean receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback, ReplyToAddressCallback<S> replyToAddressCallback) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Message> sendAndReceive(Message message) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Message> sendAndReceive(String routingKey, Message message) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Message> sendAndReceive(String exchange, String routingKey, Message message) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(Object object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(String routingKey, Object object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(String exchange, String routingKey, Object object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(Object object, MessagePostProcessor messagePostProcessor) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(String routingKey, Object object, MessagePostProcessor messagePostProcessor) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(String exchange, String routingKey, Object object, @Nullable MessagePostProcessor messagePostProcessor) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(Object object, ParameterizedTypeReference<C> responseType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(String routingKey, Object object, ParameterizedTypeReference<C> responseType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(String exchange, String routingKey, Object object, ParameterizedTypeReference<C> responseType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(Object object, MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<C> responseType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(String routingKey, Object object, @Nullable MessagePostProcessor messagePostProcessor, @Nullable ParameterizedTypeReference<C> responseType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(String exchange, String routingKey, Object object, @Nullable MessagePostProcessor messagePostProcessor, @Nullable ParameterizedTypeReference<C> responseType) {
		throw new UnsupportedOperationException();
	}

}
