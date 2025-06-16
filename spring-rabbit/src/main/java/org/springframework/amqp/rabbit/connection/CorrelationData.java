/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Correlation;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.util.Assert;

/**
 * Base class for correlating publisher confirms to sent messages. Use the
 * {@link org.springframework.amqp.rabbit.core.RabbitTemplate} methods that include one of
 * these as a parameter; when the publisher confirm is received, the CorrelationData is
 * returned with the ack/nack. When returns are also enabled, the
 * {@link #setReturned(ReturnedMessage) returned} property will be populated when a
 * message can't be delivered - the return always arrives before the confirmation. In this
 * case the {@code #id} property must be set to a unique value. If no id is provided it
 * will automatically set to a unique value.
 *
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public class CorrelationData implements Correlation {

	private final CompletableFuture<Confirm> future = new CompletableFuture<>();

	private volatile String id;

	private volatile @Nullable ReturnedMessage returnedMessage;

	/**
	 * Construct an instance with a null {@code Id}.
	 * @since 1.6.7
	 */
	public CorrelationData() {
		this.id = UUID.randomUUID().toString();
	}

	/**
	 * Construct an instance with the supplied id. Must be unique if returns are enabled
	 * to allow population of the {@link #setReturned(ReturnedMessage) returned} message.
	 * @param id the id.
	 */
	public CorrelationData(String id) {
		Assert.notNull(id, "'id' cannot be null and must be unique");
		this.id = id;
	}

	/**
	 * Return the id.
	 * @return the id.
	 */
	public String getId() {
		return this.id;
	}

	/**
	 * Set the correlation id. Generally, the correlation id shouldn't be changed.
	 * One use case, however, is when it needs to be set in a
	 * {@link org.springframework.amqp.core.MessagePostProcessor}.
	 * @param id the id.
	 * @since 1.6
	 */
	public void setId(String id) {
		Assert.notNull(id, "'id' cannot be null and must be unique");
		this.id = id;
	}

	/**
	 * Return a future to check the success/failure of the publish operation.
	 * @return the future.
	 * @since 2.1
	 */
	public CompletableFuture<Confirm> getFuture() {
		return this.future;
	}

	/**
	 * Get the returned message and metadata, if any. Guaranteed to be populated before
	 * the future is completed.
	 * @return the {@link ReturnedMessage}.
	 * @since 2.3.3
	 */
	public @Nullable ReturnedMessage getReturned() {
		return this.returnedMessage;
	}

	/**
	 * Set the returned message and metadata.
	 * @param returned the {@link ReturnedMessage}.
	 * @since 2.3.3
	 */
	public void setReturned(ReturnedMessage returned) {
		this.returnedMessage = returned;
	}

	@Override
	public String toString() {
		return "CorrelationData [id=" + this.id + "]";
	}

	/**
	 * Represents a publisher confirmation. When the ack field is
	 * true, the publish was successful; otherwise failed with a possible
	 * reason (maybe null, meaning unknown).
	 *
	 * @param ack true to confirm
	 * @param reason the reason for nack
	 *
	 * @since 2.1
	 */
	public record Confirm(boolean ack, @Nullable String reason) {

		@Deprecated(forRemoval = true, since = "4.0")
		public boolean isAck() {
			return this.ack;
		}

		@Deprecated(forRemoval = true, since = "4.0")
		public @Nullable String getReason() {
			return this.reason;
		}

	}

}
