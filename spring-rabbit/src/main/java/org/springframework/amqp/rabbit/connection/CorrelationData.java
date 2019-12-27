/*
 * Copyright 2002-2019 the original author or authors.
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

import org.springframework.amqp.core.Correlation;
import org.springframework.amqp.core.Message;
import org.springframework.lang.Nullable;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * Base class for correlating publisher confirms to sent messages.
 * Use the {@link org.springframework.amqp.rabbit.core.RabbitTemplate}
 * methods that include one of
 * these as a parameter; when the publisher confirm is received,
 * the CorrelationData is returned with the ack/nack.
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public class CorrelationData implements Correlation {

	private final SettableListenableFuture<Confirm> future = new SettableListenableFuture<>();

	@Nullable
	private volatile String id;

	private volatile Message returnedMessage;

	/**
	 * Construct an instance with a null Id.
	 * @since 1.6.7
	 */
	public CorrelationData() {
	}

	/**
	 * Construct an instance with the supplied id. Must be unique if returns are enabled
	 * to allow population of the {@link #setReturnedMessage(Message) returnedMessage}.
	 * @param id the id.
	 */
	public CorrelationData(String id) {
		this.id = id;
	}

	@Nullable
	public String getId() {
		return this.id;
	}

	/**
	 * Set the correlation id. Generally, the correlation id shouldn't be changed.
	 * One use case, however, is when it needs to be set in a
	 * {@link org.springframework.amqp.core.MessagePostProcessor} after a
	 * {@link CorrelationData} with a 'null' correlation id has been passed into a
	 * {@link org.springframework.amqp.rabbit.core.RabbitTemplate}.
	 *
	 * @param id the id.
	 * @since 1.6
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * Return a future to check the success/failure of the publish operation.
	 * @return the future.
	 * @since 2.1
	 */
	public SettableListenableFuture<Confirm> getFuture() {
		return this.future;
	}

	/**
	 * Return a returned message, if any; requires a unique
	 * {@link #CorrelationData(String) id}. Guaranteed to be populated before the future
	 * is set.
	 * @return the message or null.
	 * @since 2.1
	 */
	@Nullable
	public Message getReturnedMessage() {
		return this.returnedMessage;
	}

	/**
	 * Set a returned message for this correlation data.
	 * @param returnedMessage the returned message.
	 * @since 1.7.13
	 */
	public void setReturnedMessage(Message returnedMessage) {
		this.returnedMessage = returnedMessage;
	}

	@Override
	public String toString() {
		return "CorrelationData [id=" + this.id + "]";
	}

	/**
	 * Represents a publisher confirmation. When the ack field is
	 * true, the publish was successful; otherwise failed with a possible
	 * reason (may be null, meaning unknown).
	 *
	 * @since 2.1
	 */
	public static class Confirm {

		private final boolean ack;

		private final String reason;

		public Confirm(boolean ack, @Nullable String reason) {
			this.ack = ack;
			this.reason = reason;
		}

		public boolean isAck() {
			return this.ack;
		}

		public String getReason() {
			return this.reason;
		}

		@Override
		public String toString() {
			return "Confirm [ack=" + this.ack
					+ (this.reason != null ? ", reason=" + this.reason : "")
					+ "]";
		}

	}

}
