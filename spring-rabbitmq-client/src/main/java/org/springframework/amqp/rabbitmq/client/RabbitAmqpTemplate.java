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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.Resource;
import com.rabbitmq.client.amqp.RpcClient;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.AsyncAmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReceiveAndReplyCallback;
import org.springframework.amqp.core.ReceiveAndReplyMessageCallback;
import org.springframework.amqp.rabbit.core.AmqpNackReceivedException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.converter.SmartMessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * The {@link AmqpTemplate} for RabbitMQ AMQP 1.0 protocol support.
 * A Spring-friendly wrapper around {@link Environment#connectionBuilder()};
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public class RabbitAmqpTemplate implements AsyncAmqpTemplate, DisposableBean {

	private static final LogAccessor LOG = new LogAccessor(RabbitAmqpAdmin.class);

	private final AmqpConnectionFactory connectionFactory;

	private final Lock instanceLock = new ReentrantLock();

	private @Nullable Object publisher;

	private MessageConverter messageConverter = new SimpleMessageConverter();

	private @Nullable String defaultExchange;

	private @Nullable String defaultRoutingKey;

	private @Nullable String defaultQueue;

	private @Nullable String defaultReceiveQueue;

	private @Nullable String defaultReplyToQueue;

	private Resource.StateListener @Nullable [] stateListeners;

	private Duration publishTimeout = Duration.ofSeconds(60);

	private Duration completionTimeout = Duration.ofSeconds(60);

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
	 * Set a duration for {@link CompletableFuture#orTimeout(long, TimeUnit)} on returns.
	 * There is no {@link CompletableFuture} API like {@code onTimeout()} requested
	 * from the {@link CompletableFuture#get(long, TimeUnit)},
	 * but used in operations AMQP resources have to be closed eventually independently
	 * of the {@link CompletableFuture} fulfilment.
	 * Defaults to 1 minute.
	 * @param completionTimeout duration for future completions.
	 */
	public void setCompletionTimeout(Duration completionTimeout) {
		this.completionTimeout = completionTimeout;
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
	 * The name of the default queue to receive messages from when none is specified explicitly.
	 * @param queue the default queue name to use for receive operation.
	 */
	public void setReceiveQueue(String queue) {
		this.defaultReceiveQueue = queue;
	}

	/**
	 * The name of the default queue to receive replies from when none is specified explicitly.
	 * @param queue the default queue name to use for send-n-receive operation.
	 */
	public void setReplyToQueue(String queue) {
		this.defaultReplyToQueue = queue;
	}

	/**
	 * Set a converter for {@link #convertAndSend(Object)} operations.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
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
				"For send with defaults, an 'exchange' (and optional 'routingKey') or 'queue' must be provided");
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

		com.rabbitmq.client.amqp.Message amqpMessage =
				toAmqpMessage(exchange, routingKey, queue, message, getPublisher()::message);

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
				"For send with defaults, an 'exchange' (and optional 'routingKey') or 'queue' must be provided");
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

		Message message = convertToMessageIfNecessary(data);
		if (messagePostProcessor != null) {
			message = messagePostProcessor.postProcessMessage(message);
		}
		return doSend(exchange, routingKey, queue, message);
	}

	@Override
	public CompletableFuture<Message> receive() {
		return receive(getRequiredQueue());
	}

	/**
	 * Request a head message from the provided queue.
	 * A returned {@link CompletableFuture} timeouts after {@link #setCompletionTimeout(Duration)}.
	 * @param queueName the queue to consume message from.
	 * @return the future with a received message.
	 * @see #setCompletionTimeout(Duration)
	 */
	@SuppressWarnings("try")
	@Override
	public CompletableFuture<Message> receive(String queueName) {
		CompletableFuture<Message> messageFuture = new CompletableFuture<>();

		AtomicBoolean messageReceived = new AtomicBoolean();

		Consumer consumer =
				this.connectionFactory.getConnection()
						.consumerBuilder()
						.queue(queueName)
						.initialCredits(1)
						.priority(10)
						.messageHandler((context, message) -> {
							if (messageReceived.compareAndSet(false, true)) {
								context.accept();
								messageFuture.complete(RabbitAmqpUtils.fromAmqpMessage(message, null));
							}
						})
						.build();

		return messageFuture
				.orTimeout(this.completionTimeout.toMillis(), TimeUnit.MILLISECONDS)
				.whenComplete((message, exception) -> consumer.close());
	}

	@Override
	public CompletableFuture<Object> receiveAndConvert() {
		return receiveAndConvert((ParameterizedTypeReference<Object>) null);
	}

	@Override
	public CompletableFuture<Object> receiveAndConvert(String queueName) {
		return receiveAndConvert(queueName, null);
	}

	/**
	 * Receive a message from {@link #setReceiveQueue(String)} and convert its body
	 * to the expected type.
	 * The {@link #setMessageConverter(MessageConverter)} must be an implementation of {@link SmartMessageConverter}.
	 * @param type the type to covert received result.
	 * @return the CompletableFuture with a result.
	 */
	@Override
	public <T> CompletableFuture<T> receiveAndConvert(@Nullable ParameterizedTypeReference<T> type) {
		return receiveAndConvert(getRequiredQueue(), type);
	}

	/**
	 * Receive a message from {@link #setReceiveQueue(String)} and convert its body
	 * to the expected type.
	 * The {@link #setMessageConverter(MessageConverter)} must be an implementation of {@link SmartMessageConverter}.
	 * @param queueName the queue to consume message from.
	 * @param type the type to covert received result.
	 * @return the CompletableFuture with a result.
	 */
	@Override
	public <T> CompletableFuture<T> receiveAndConvert(String queueName, @Nullable ParameterizedTypeReference<T> type) {
		return receive(queueName)
				.thenApply((message) -> convertReply(message, type));
	}

	@SuppressWarnings("unchecked")
	private <T> T convertReply(Message message, @Nullable ParameterizedTypeReference<T> type) {
		if (type != null) {
			return (T) getRequiredSmartMessageConverter().fromMessage(message, type);
		}
		else {
			return (T) this.messageConverter.fromMessage(message);
		}
	}

	private SmartMessageConverter getRequiredSmartMessageConverter() throws IllegalStateException {
		Assert.state(this.messageConverter instanceof SmartMessageConverter,
				"template's message converter must be a SmartMessageConverter");
		return (SmartMessageConverter) this.messageConverter;
	}

	@Override
	public <R, S> CompletableFuture<Boolean> receiveAndReply(ReceiveAndReplyCallback<R, S> callback) {
		return receiveAndReply(getRequiredQueue(), callback);
	}

	@Override
	@SuppressWarnings("try")
	public <R, S> CompletableFuture<Boolean> receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback) {
		CompletableFuture<Boolean> rpcFuture = new CompletableFuture<>();

		Consumer.MessageHandler consumerHandler =
				(context, message) -> {
					Message requestMessage = RabbitAmqpUtils.fromAmqpMessage(message, null);
					try {
						Object messageId = message.messageId();
						Assert.notNull(messageId,
								"The 'message-id' property has to be set on request. Used for reply correlation.");
						String replyTo = message.replyTo();
						Assert.hasText(replyTo,
								"The 'reply-to' property has to be set on request. Used for reply publishing.");
						Message reply = handleRequestAndProduceReply(requestMessage, callback);
						if (reply == null) {
							LOG.info(() -> "No reply for request: " + requestMessage);
							context.accept();
							rpcFuture.complete(false);
						}
						else {
							com.rabbitmq.client.amqp.Message replyMessage = getPublisher().message();
							RabbitAmqpUtils.toAmqpMessage(reply, replyMessage);
							replyMessage.correlationId(messageId);
							replyMessage.to(replyTo);
							getPublisher().publish(replyMessage, (ctx) -> {
							});
							context.accept();
							rpcFuture.complete(true);
						}
					}
					catch (Exception ex) {
						context.discard();
						rpcFuture.completeExceptionally(
								new AmqpIllegalStateException("Failed to process RPC request: " + requestMessage, ex));
					}
				};

		Consumer consumer =
				this.connectionFactory.getConnection()
						.consumerBuilder()
						.queue(queueName)
						.initialCredits(1)
						.priority(10)
						.messageHandler(consumerHandler)
						.build();

		return rpcFuture
				.orTimeout(this.completionTimeout.toMillis(), TimeUnit.MILLISECONDS)
				.whenComplete((message, exception) -> consumer.close());
	}

	@SuppressWarnings("unchecked")
	private <S, R> @Nullable Message handleRequestAndProduceReply(Message requestMessage,
			ReceiveAndReplyCallback<R, S> callback) {

		Object receive = requestMessage;
		if (!(ReceiveAndReplyMessageCallback.class.isAssignableFrom(callback.getClass()))) {
			receive = this.messageConverter.fromMessage(requestMessage);
		}

		S reply;
		try {
			reply = callback.handle((R) receive);
		}
		catch (ClassCastException ex) {
			StackTraceElement[] trace = ex.getStackTrace();
			if (trace[0].getMethodName().equals("handle")
					&& Objects.equals(trace[1].getFileName(), "RabbitAmqpTemplate.java")) {

				throw new IllegalArgumentException("ReceiveAndReplyCallback '" + callback
						+ "' can't handle received object '" + receive + "'", ex);
			}
			else {
				throw ex;
			}
		}

		if (reply != null) {
			return convertToMessageIfNecessary(reply);
		}
		return null;
	}

	private Message convertToMessageIfNecessary(Object data) {
		if (data instanceof Message msg) {
			return msg;
		}
		else {
			return this.messageConverter.toMessage(data, new MessageProperties());
		}
	}

	@Override
	public CompletableFuture<Message> sendAndReceive(Message message) {
		Assert.state(this.defaultExchange != null || this.defaultQueue != null,
				"For send-n-receive with defaults, an 'exchange' (and optional 'routingKey') or 'queue' must be provided");
		return doSendAndReceive(this.defaultExchange, this.defaultRoutingKey, this.defaultQueue, message);
	}

	@Override
	public CompletableFuture<Message> sendAndReceive(String exchange, @Nullable String routingKey, Message message) {
		return doSendAndReceive(exchange, routingKey != null ? routingKey : this.defaultRoutingKey, null, message);
	}

	@Override
	public CompletableFuture<Message> sendAndReceive(String queue, Message message) {
		return doSendAndReceive(null, null, queue, message);
	}

	@SuppressWarnings("try")
	private CompletableFuture<Message> doSendAndReceive(@Nullable String exchange, @Nullable String routingKey,
			@Nullable String queue, Message message) {

		MessageProperties messageProperties = message.getMessageProperties();
		String messageId = messageProperties.getMessageId();
		String correlationId = messageProperties.getCorrelationId();
		String replyTo = messageProperties.getReplyTo();

		// HTTP over AMQP 1.0 extension specification, 5.1:
		// To associate a response with a request, the correlation-id value of the response properties
		// MUST be set to the message-id value of the request properties.
		// So, this supplier will override request message-id, respectively.
		// Otherwise, the RpcClient generates correlation-id internally.
		Supplier<Object> correlationIdSupplier = null;
		if (StringUtils.hasText(correlationId)) {
			correlationIdSupplier = () -> correlationId;
		}
		else if (StringUtils.hasText(messageId)) {
			correlationIdSupplier = () -> messageId;
		}

		// The default reply-to queue, or the one supplied in the message.
		// Otherwise, the RpcClient generates one as exclusive and auto-delete.
		String replyToQueue = this.defaultReplyToQueue;
		if (StringUtils.hasText(replyTo)) {
			replyToQueue = replyTo;
		}

		RpcClient rpcClient =
				this.connectionFactory.getConnection()
						.rpcClientBuilder()
						.requestTimeout(this.publishTimeout)
						.correlationIdSupplier(correlationIdSupplier)
						.replyToQueue(replyToQueue)
						.build();

		com.rabbitmq.client.amqp.Message amqpMessage =
				toAmqpMessage(exchange, routingKey, queue, message, rpcClient::message);

		return rpcClient.publish(amqpMessage)
				.thenApply((reply) -> RabbitAmqpUtils.fromAmqpMessage(reply, null))
				.orTimeout(this.completionTimeout.toMillis(), TimeUnit.MILLISECONDS)
				.whenComplete((replyMessage, exception) -> rpcClient.close());
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(Object object) {
		return convertSendAndReceiveAsType(object, null, null);
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(String queue, Object object) {
		return convertSendAndReceiveAsType(queue, object, null, null);
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(String exchange, @Nullable String routingKey, Object object) {
		return convertSendAndReceiveAsType(exchange, routingKey, object, null, null);
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(Object object, MessagePostProcessor messagePostProcessor) {
		return convertSendAndReceiveAsType(object, messagePostProcessor, null);
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(String queue, Object object,
			MessagePostProcessor messagePostProcessor) {

		return convertSendAndReceiveAsType(queue, object, messagePostProcessor, null);
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceive(String exchange, @Nullable String routingKey,
			Object object, @Nullable MessagePostProcessor messagePostProcessor) {

		return convertSendAndReceiveAsType(exchange, routingKey, object, messagePostProcessor, null);
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(Object object,
			ParameterizedTypeReference<C> responseType) {

		return convertSendAndReceiveAsType(object, null, responseType);
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(String queue, Object object,
			ParameterizedTypeReference<C> responseType) {

		return convertSendAndReceiveAsType(queue, object, null, responseType);
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(String exchange, @Nullable String routingKey,
			Object object, ParameterizedTypeReference<C> responseType) {

		return convertSendAndReceiveAsType(exchange, routingKey, object, null, responseType);
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(Object object,
			@Nullable MessagePostProcessor messagePostProcessor, @Nullable ParameterizedTypeReference<C> responseType) {

		Assert.state(this.defaultExchange != null || this.defaultQueue != null,
				"For send with defaults, an 'exchange' (and optional 'routingKey') or 'queue' must be provided");

		return doConvertSendAndReceive(this.defaultExchange, this.defaultRoutingKey, this.defaultQueue, object,
				messagePostProcessor, responseType);
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(String queue, Object object,
			@Nullable MessagePostProcessor messagePostProcessor, @Nullable ParameterizedTypeReference<C> responseType) {

		return doConvertSendAndReceive(null, null, queue, object, messagePostProcessor, responseType);
	}

	@Override
	public <C> CompletableFuture<C> convertSendAndReceiveAsType(String exchange, @Nullable String routingKey,
			Object object, @Nullable MessagePostProcessor messagePostProcessor,
			@Nullable ParameterizedTypeReference<C> responseType) {

		return doConvertSendAndReceive(exchange, routingKey != null ? routingKey : this.defaultRoutingKey, null,
				object, messagePostProcessor, responseType);
	}

	private <C> CompletableFuture<C> doConvertSendAndReceive(@Nullable String exchange, @Nullable String routingKey,
			@Nullable String queue, Object data, @Nullable MessagePostProcessor messagePostProcessor,
			@Nullable ParameterizedTypeReference<C> responseType) {

		Message message = convertToMessageIfNecessary(data);
		if (messagePostProcessor != null) {
			message = messagePostProcessor.postProcessMessage(message);
		}
		return doSendAndReceive(exchange, routingKey, queue, message)
				.thenApply((reply) -> convertReply(reply, responseType));
	}

	private static com.rabbitmq.client.amqp.Message toAmqpMessage(@Nullable String exchange,
			@Nullable String routingKey, @Nullable String queue, Message message,
			Supplier<com.rabbitmq.client.amqp.Message> amqpMessageSupplier) {

		com.rabbitmq.client.amqp.Message.MessageAddressBuilder address =
				amqpMessageSupplier.get()
						.toAddress();

		JavaUtils.INSTANCE
				.acceptIfNotNull(exchange, address::exchange)
				.acceptIfNotNull(routingKey, address::key)
				.acceptIfNotNull(queue, address::queue);

		com.rabbitmq.client.amqp.Message amqpMessage = address.message();

		RabbitAmqpUtils.toAmqpMessage(message, amqpMessage);

		return amqpMessage;
	}

}
