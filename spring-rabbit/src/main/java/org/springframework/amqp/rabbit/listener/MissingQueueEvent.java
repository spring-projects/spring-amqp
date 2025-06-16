/*
 * Copyright 2021-present the original author or authors.
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

import org.springframework.amqp.event.AmqpEvent;

/**
 * Event published when a missing queue is detected.
 *
 * @author Gary Russell
 * @since 2.1.18
 *
 */
public class MissingQueueEvent extends AmqpEvent {

	private static final long serialVersionUID = 1L;

	private final String queue;

	/**
	 * Construct an instance with the provided source and queue.
	 * @param source the source.
	 * @param queue the queue.
	 */
	public MissingQueueEvent(Object source, String queue) {
		super(source);
		this.queue = queue;
	}

	/**
	 * Return the missing queue.
	 * @return the queue.
	 */
	public String getQueue() {
		return this.queue;
	}

	@Override
	public String toString() {
		return "MissingQueueEvent [queue=" + this.queue + ", source=" + this.source + "]";
	}

}
