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

import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.utils.JavaUtils;

/**
 * The fluent API for the AMQP 1.0 client.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public interface AmqpClient {

	/**
	 * Send a ProtonJ message instance.
	 * The {@link Message#to()} can be omitted if {@link AmqpClient.Builder#defaultToAddress(String)} is provided.
	 * @param protonMessage the message to send.
	 * @return message settlement result.
	 */
	CompletableFuture<Boolean> send(Message<?> protonMessage);

	/**
	 * Send a Spring AMQP message.
	 * Converted internally to a ProtonJ message instance.
	 * This is a convenient API based on the {@link AmqpClient.Builder#defaultToAddress(String)}
	 * @param message the message to send.
	 * @return message settlement result.
	 */
	CompletableFuture<Boolean> sendToDefault(org.springframework.amqp.core.Message message);

	/**
	 * The fluent API for the send operation based on the message building blocks starting with {@code to} address.
	 * @param toAddress the target AMQP 1.0 address.
	 * @return the {@link SendSpec} with the message building blocks.
	 */
	SendSpec to(String toAddress);

	static AmqpClient create(AmqpConnectionFactory connectionFactory) {
		return builder(connectionFactory).build();
	}

	static Builder builder(AmqpConnectionFactory connectionFactory) {
		return new Builder(connectionFactory);
	}

	class Builder {

		private final AmqpConnectionFactory connectionFactory;

		private @Nullable SenderOptions senderOptions;

		private @Nullable MessageConverter messageConverter;

		private @Nullable String defaultToAddress;

		Builder(AmqpConnectionFactory connectionFactory) {
			this.connectionFactory = connectionFactory;
		}

		/**
		 * Set the {@link SenderOptions} for an internal AMQP 1.0 {@link org.apache.qpid.protonj2.client.Sender}.
		 * @param senderOptions to use.
		 * @return this builder.
		 */
		public Builder senderOptions(SenderOptions senderOptions) {
			this.senderOptions = senderOptions;
			return this;
		}

		/**
		 * Set the default target AMQP 1.0 address for the
		 * {@link #sendToDefault(org.springframework.amqp.core.Message)} and {@link #send(Message)} operations.
		 * @param defaultToAddress the target address to use a convenient default.
		 * @return this builder.
		 */
		public Builder defaultToAddress(String defaultToAddress) {
			this.defaultToAddress = defaultToAddress;
			return this;
		}

		/**
		 * Set the {@link MessageConverter} for converting to/from message body.
		 * @param messageConverter the converter.
		 * @return this builder.
		 */
		public Builder messageConverter(MessageConverter messageConverter) {
			this.messageConverter = messageConverter;
			return this;
		}

		/**
		 * Build the {@link AmqpClient} instance based on the provided options.
		 * @return the client instance.
		 */
		public AmqpClient build() {
			DefaultAmqpClient defaultAmqpClient = new DefaultAmqpClient(this.connectionFactory);
			JavaUtils.INSTANCE
							.acceptIfNotNull(this.senderOptions, defaultAmqpClient::setSenderOptions)
							.acceptIfNotNull(this.messageConverter, defaultAmqpClient::setMessageConverter)
							.acceptIfNotNull(this.defaultToAddress, defaultAmqpClient::setDefaultToAddress);
			return defaultAmqpClient;
		}

	}

	/**
	 * The fluent API for the send operation based on the message building blocks starting with {@code to} address.
	 */
	interface SendSpec {

		default MessageSendSpec message(org.springframework.amqp.core.Message message) {
			return protonMessage(ProtonUtils.toProtonMessage(message));
		}

		/**
		 * Create a {@link MessageSendSpec} from a ProtonJ message instance.
		 * The default implementation sets the {@link Message#to()} from the provided {@link AmqpClient#to(String)}.
		 * @param protonMessage the message to send.
		 * @return the {@link MessageSendSpec}.
		 */
		MessageSendSpec protonMessage(Message<?> protonMessage);

		/**
		 * Create a {@link ConvertAndSendSpec} from the provided body.
		 * @param body the payload to be converted into a message body.
		 * @return the {@link ConvertAndSendSpec}.
		 */
		ConvertAndSendSpec body(Object body);

		/**
		 * The last step in the fluent API for the send operation.
		 */
		interface MessageSendSpec {

			/**
			 * Perform the send operation of the message provided in the previous step from {@link SendSpec}.
			 * @return the message settlement result.
			 */
			CompletableFuture<Boolean> send();

		}

		/**
		 * The fluent API for the send operation based on the message building blocks starting with {@code to} address.
		 */
		interface ConvertAndSendSpec {

			ConvertAndSendSpec priority(int priority);

			ConvertAndSendSpec timeToLive(Duration expiration);

			ConvertAndSendSpec durable(boolean durable);

			ConvertAndSendSpec contentType(String contentType);

			ConvertAndSendSpec contentEncoding(String contentEncoding);

			ConvertAndSendSpec replyTo(String replyTo);

			ConvertAndSendSpec correlationId(String correlationId);

			ConvertAndSendSpec userId(String userId);

			ConvertAndSendSpec messageId(String messageId);

			ConvertAndSendSpec creationTime(Date creationTime);

			ConvertAndSendSpec header(String key, Object value);

			/**
			 * Perform the send operation of the message built from the properties provided above.
			 * @return the message settlement result.
			 */
			CompletableFuture<Boolean> send();

		}

	}

}
