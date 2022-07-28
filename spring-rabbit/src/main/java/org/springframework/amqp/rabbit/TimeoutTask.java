/*
 * Copyright 2022 the original author or authors.
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

import java.util.concurrent.ConcurrentMap;

import org.springframework.amqp.core.AmqpReplyTimeoutException;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;
import org.springframework.lang.Nullable;

/**
 * A {@link Runnable} used to time out a {@link RabbitFuture}.
 *
 * @author Gary Russell
 * @since 2.4.7
 */
public class TimeoutTask implements Runnable {

	private final RabbitFuture<?> future;

	private final ConcurrentMap<String, RabbitFuture<?>> pending;

	private final DirectReplyToMessageListenerContainer container;

	TimeoutTask(RabbitFuture<?> future, ConcurrentMap<String, RabbitFuture<?>> pending,
			@Nullable DirectReplyToMessageListenerContainer container) {

		this.future = future;
		this.pending = pending;
		this.container = container;
	}

	@Override
	public void run() {
		this.pending.remove(this.future.getCorrelationId());
		ChannelHolder holder = this.future.getChannelHolder();
		if (holder != null && this.container != null) {
			this.container.releaseConsumerFor(holder, false, null); // NOSONAR
		}
		this.future.completeExceptionally(
				new AmqpReplyTimeoutException("Reply timed out", this.future.getRequestMessage()));
	}

}
