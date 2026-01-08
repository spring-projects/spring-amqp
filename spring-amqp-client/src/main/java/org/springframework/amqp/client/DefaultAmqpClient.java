/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.converter.SmartMessageConverter;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

/**
 * An internal, default implementation of {@link AmqpClient}.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
class DefaultAmqpClient implements AmqpClient, DisposableBean {

	private final AmqpConnectionFactory connectionFactory;

	private final Lock instanceLock = new ReentrantLock();

	private final ReceiverOptions receiverOptions = new ReceiverOptions()
			// The 'receive' consumers are volatile and only about one message to consume.
			// Therefore, no initial credit is needed:
			// only one is permitted explicitly in the 'Receiver' instance.
			.creditWindow(0);

	private SenderOptions senderOptions = new SenderOptions();

	private MessageConverter messageConverter = new SimpleMessageConverter();

	private Duration completionTimeout = Duration.ofSeconds(60);

	private TaskExecutor taskExecutor = new ThreadPoolTaskExecutor();

	private boolean taskExecutorSet;

	private @Nullable String defaultToAddress;

	private volatile @Nullable Sender sender;

	DefaultAmqpClient(AmqpConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
		((ThreadPoolTaskExecutor) this.taskExecutor).afterPropertiesSet();
	}

	void setSenderOptions(SenderOptions senderOptions) {
		this.senderOptions = senderOptions;
	}

	void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	void setDefaultToAddress(String defaultToAddress) {
		this.defaultToAddress = defaultToAddress;
	}

	void setCompletionTimeout(Duration completionTimeout) {
		this.completionTimeout = completionTimeout;
	}

	void setTaskExecutor(TaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
		this.taskExecutorSet = true;
	}

	@Override
	public CompletableFuture<Boolean> sendToDefault(org.springframework.amqp.core.Message message) {
		return send(ProtonUtils.toProtonMessage(message));
	}

	@Override
	public CompletableFuture<Boolean> send(Message<?> message) {
		Future<Tracker> trackerFuture;
		try {
			String toAddress = message.to();
			if (toAddress == null) {
				Assert.state(this.defaultToAddress != null, "The 'to' address is not supplied, and no default");
				message.to(this.defaultToAddress);
			}
			trackerFuture = getSender().send(message).settlementFuture();
		}
		catch (ClientException ex) {
			throw ProtonUtils.toAmqpException(ex);
		}

		Supplier<Tracker> supplier = ProtonUtils.toSupplier(trackerFuture, this.senderOptions.sendTimeout());
		return CompletableFuture.supplyAsync(supplier, this.taskExecutor)
				.thenApply((tracker) -> {
					DeliveryState.Type deliveryStateType = tracker.remoteState().getType();
					if (DeliveryState.Type.ACCEPTED.equals(deliveryStateType)) {
						return true;
					}
					throw new AmqpClientNackReceivedException("The message was not accepted, but " + deliveryStateType,
							message);
				});
	}

	@Override
	public SendSpec to(String toAddress) {
		return new DefaultSendSpec(toAddress);
	}

	@Override
	public ReceiveSpec from(String fromAddress) {
		return new DefaultReceiveSpec(fromAddress);
	}

	private Sender getSender() throws ClientException {
		Sender senderToReturn = this.sender;
		if (senderToReturn == null) {
			this.instanceLock.lock();
			try {
				senderToReturn = this.sender;
				if (senderToReturn == null) {
					senderToReturn =
							this.connectionFactory
									.getConnection()
									.openAnonymousSender(this.senderOptions);
					this.sender = senderToReturn;
				}
			}
			finally {
				this.instanceLock.unlock();
			}
		}
		return senderToReturn;
	}

	private <M extends Message<?>> CompletableFuture<M> receive(String fromAddress, @Nullable Duration receiveTimeout) {
		try {
			Receiver receiver =
					this.connectionFactory.getConnection()
							.openReceiver(fromAddress, this.receiverOptions)
							// Since this 'Receiver' is volatile and only about one message to consume,
							// therefore only one credit is permitted without renewing.
							.addCredit(1);

			Supplier<Delivery> supplier =
					() -> {
						try {
							if (receiveTimeout != null) {
								return receiver.receive(receiveTimeout.toMillis(), TimeUnit.MILLISECONDS);
							}
							else {
								return receiver.receive();
							}
						}
						catch (ClientException ex) {
							throw ProtonUtils.toAmqpException(ex);
						}
					};

			return CompletableFuture.supplyAsync(supplier, this.taskExecutor)
					.<M>thenApply(DefaultAmqpClient::deliveryToMessage)
					.orTimeout(this.completionTimeout.toMillis(), TimeUnit.MILLISECONDS)
					.whenComplete((message, exception) -> receiver.close());
		}
		catch (ClientException ex) {
			throw ProtonUtils.toAmqpException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	private static <M> M deliveryToMessage(Delivery delivery) {
		try {
			return (M) delivery.message();
		}
		catch (ClientException ex) {
			throw ProtonUtils.toAmqpException(ex);
		}
	}

	@Override
	public void destroy() {
		Sender senderToClose = this.sender;
		if (senderToClose != null) {
			senderToClose.close();
			this.sender = null;
		}

		if (!this.taskExecutorSet) {
			((ThreadPoolTaskExecutor) this.taskExecutor).destroy();
		}
	}

	class DefaultSendSpec implements SendSpec {

		private final String toAddress;

		DefaultSendSpec(String toAddress) {
			this.toAddress = toAddress;
		}

		@Override
		public MessageSendSpec protonMessage(Message<?> protonMessage) {
			setTo(protonMessage);
			return new DefaultMessageSendSpec(protonMessage);
		}

		private void setTo(Message<?> protonMessage) {
			try {
				protonMessage.to(this.toAddress);
			}
			catch (ClientException ex) {
				throw ProtonUtils.toAmqpException(ex);
			}
		}

		@Override
		public ConvertAndSendSpec body(Object body) {
			return new DefaultConvertAndSendSpec(body);
		}

		class DefaultMessageSendSpec implements MessageSendSpec {

			private final Message<?> message;

			DefaultMessageSendSpec(Message<?> message) {
				this.message = message;
			}

			@Override
			public CompletableFuture<Boolean> send() {
				return DefaultAmqpClient.this.send(this.message);
			}

		}

		class DefaultConvertAndSendSpec implements ConvertAndSendSpec {

			private final Object body;

			private final MessagePropertiesBuilder messagePropertiesBuilder = MessagePropertiesBuilder.newInstance();

			DefaultConvertAndSendSpec(Object body) {
				this.body = body;
			}

			@Override
			public ConvertAndSendSpec priority(int priority) {
				this.messagePropertiesBuilder.setPriority(priority);
				return this;
			}

			@Override
			public ConvertAndSendSpec timeToLive(Duration expiration) {
				this.messagePropertiesBuilder.setExpiration("" + expiration.toMillis());
				return this;
			}

			@Override
			public ConvertAndSendSpec durable(boolean durable) {
				if (!durable) { // default is MessageDeliveryMode.PERSISTENT
					this.messagePropertiesBuilder.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
				}
				return this;
			}

			/**
			 * Set a content type of the message to send.
			 * Could be overridden by the {@link MessageConverter} if body is not a {@code byte[]}.
			 * @param contentType to use.
			 * @return the spec.
			 */
			@Override
			public ConvertAndSendSpec contentType(String contentType) {
				this.messagePropertiesBuilder.setContentType(contentType);
				return this;
			}

			@Override
			public ConvertAndSendSpec contentEncoding(String contentEncoding) {
				this.messagePropertiesBuilder.setContentEncoding(contentEncoding);
				return this;
			}

			@Override
			public ConvertAndSendSpec replyTo(String replyTo) {
				this.messagePropertiesBuilder.setReplyTo(replyTo);
				return this;
			}

			@Override
			public ConvertAndSendSpec correlationId(String correlationId) {
				this.messagePropertiesBuilder.setCorrelationId(correlationId);
				return this;
			}

			@Override
			public ConvertAndSendSpec userId(String userId) {
				this.messagePropertiesBuilder.setUserId(userId);
				return this;
			}

			@Override
			public ConvertAndSendSpec messageId(String messageId) {
				this.messagePropertiesBuilder.setMessageId(messageId);
				return this;
			}

			@Override
			public ConvertAndSendSpec creationTime(Date creationTime) {
				this.messagePropertiesBuilder.setTimestamp(creationTime);
				return this;
			}

			@Override
			public ConvertAndSendSpec header(String key, Object value) {
				this.messagePropertiesBuilder.setHeader(key, value);
				return this;
			}

			@Override
			public CompletableFuture<Boolean> send() {
				MessageProperties messageProperties = this.messagePropertiesBuilder.build();
				org.springframework.amqp.core.Message message =
						this.body instanceof byte[] bytesBody
								? new org.springframework.amqp.core.Message(bytesBody, messageProperties)
								: DefaultAmqpClient.this.messageConverter.toMessage(this.body, messageProperties);

				Message<?> protonMessage = ProtonUtils.toProtonMessage(message);
				DefaultSendSpec.this.setTo(protonMessage);
				return DefaultAmqpClient.this.send(protonMessage);
			}

		}

	}

	class DefaultReceiveSpec implements ReceiveSpec {

		private final String fromAddress;

		private @Nullable Duration receiveTimeout;

		DefaultReceiveSpec(String fromAddress) {
			this.fromAddress = fromAddress;
		}

		@Override
		public ReceiveSpec timeout(Duration timeout) {
			this.receiveTimeout = timeout;
			return this;
		}

		@Override
		public CompletableFuture<Message<?>> receiveProtonMessage() {
			return DefaultAmqpClient.this.receive(this.fromAddress, receiveTimeout);
		}

		@Override
		@SafeVarargs
		@SuppressWarnings("varargs")
		public final <T> CompletableFuture<T> receiveAndConvert(T... reified) {
			Assert.state(reified.length == 0,
					"No parameters are allowed for 'receiveAndConvert'. " +
							"The generic argument is enough for inferring the expected conversion type.");

			var parameterizedTypeReference = ParameterizedTypeReference.forType(getClassOf(reified));
			boolean isObjectType = Object.class.equals(parameterizedTypeReference.getType());
			if (!isObjectType) {
				Assert.state(DefaultAmqpClient.this.messageConverter instanceof SmartMessageConverter,
						"The client's message converter must be a 'SmartMessageConverter'");
			}

			return receive()
					.thenApply((message) ->
							convertReply(message, isObjectType ? null : parameterizedTypeReference));
		}

		@SuppressWarnings("unchecked")
		private <T> T convertReply(org.springframework.amqp.core.Message message,
				@Nullable ParameterizedTypeReference<?> type) {

			if (type == null) {
				return (T) DefaultAmqpClient.this.messageConverter.fromMessage(message);
			}
			else {
				return (T) ((SmartMessageConverter) DefaultAmqpClient.this.messageConverter).fromMessage(message, type);
			}
		}

		@SuppressWarnings("unchecked")
		private static <T> Class<T> getClassOf(T[] array) {
			return (Class<T>) array.getClass().getComponentType();
		}

	}

}
