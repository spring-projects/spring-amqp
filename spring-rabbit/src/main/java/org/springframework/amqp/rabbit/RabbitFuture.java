/*
 * Copyright 2022-2024 the original author or authors.
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

package org.springframework.amqp.rabbit;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;

/**
 * Base class for {@link CompletableFuture}s returned by {@link AsyncRabbitTemplate}.
 * @param <T> the type.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.4.7
 */
public abstract class RabbitFuture<T> extends CompletableFuture<T> {

	private final String correlationId;

	private final Message requestMessage;

	private final BiConsumer<String, ChannelHolder> canceler;

	private final Function<RabbitFuture<?>, ScheduledFuture<?>> timeoutTaskFunction;

	private ScheduledFuture<?> timeoutTask;

	private volatile CompletableFuture<Boolean> confirm;

	private String nackCause;

	private ChannelHolder channelHolder;

	protected RabbitFuture(String correlationId, Message requestMessage, BiConsumer<String, ChannelHolder> canceler,
			Function<RabbitFuture<?>, ScheduledFuture<?>> timeoutTaskFunction) {

		this.correlationId = correlationId;
		this.requestMessage = requestMessage;
		this.canceler = canceler;
		this.timeoutTaskFunction = timeoutTaskFunction;
	}

	void setChannelHolder(ChannelHolder channel) {
		this.channelHolder = channel;
	}

	String getCorrelationId() {
		return this.correlationId;
	}

	ChannelHolder getChannelHolder() {
		return this.channelHolder;
	}

	Message getRequestMessage() {
		return this.requestMessage;
	}

	@Override
	public boolean complete(T value) {
		try {
			return super.complete(value);
		}
		finally {
			cancelTimeoutTaskIfAny();
		}
	}

	@Override
	public boolean completeExceptionally(Throwable ex) {
		try {
			return super.completeExceptionally(ex);
		}
		finally {
			cancelTimeoutTaskIfAny();
		}
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		this.canceler.accept(this.correlationId, this.channelHolder);
		try {
			return super.cancel(mayInterruptIfRunning);
		}
		finally {
			cancelTimeoutTaskIfAny();
		}
	}

	private void cancelTimeoutTaskIfAny() {
		if (this.timeoutTask != null) {
			this.timeoutTask.cancel(true);
		}
	}

	/**
	 * When confirms are enabled contains a {@link CompletableFuture}
	 * for the confirmation.
	 * @return the future.
	 */
	public CompletableFuture<Boolean> getConfirm() {
		return this.confirm;
	}

	void setConfirm(CompletableFuture<Boolean> confirm) {
		this.confirm = confirm;
	}

	/**
	 * When confirms are enabled and a nack is received, contains
	 * the cause for the nack, if any.
	 * @return the cause.
	 */
	public String getNackCause() {
		return this.nackCause;
	}

	void setNackCause(String nackCause) {
		this.nackCause = nackCause;
	}

	void startTimer() {
		this.timeoutTask = this.timeoutTaskFunction.apply(this);
	}

}
