/*
 * Copyright 2015-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import java.io.Serial;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.event.AmqpEvent;

/**
 * Published when a listener consumer is terminated.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public class ListenerContainerConsumerTerminatedEvent extends AmqpEvent {

	@Serial
	private static final long serialVersionUID = -8122166328567190605L;

	private final @Nullable String reason;

	/**
	 * Construct an instance with the provided arguments.
	 * @param source the source container.
	 * @param reason the reason.
	 */
	public ListenerContainerConsumerTerminatedEvent(Object source, @Nullable String reason) {
		super(source);
		this.reason = reason;
	}

	public @Nullable String getReason() {
		return this.reason;
	}

	@Override
	public String toString() {
		return "ListenerContainerConsumerTerminatedEvent [reason=" + this.reason + ", container=" + this.source + "]";
	}

}
