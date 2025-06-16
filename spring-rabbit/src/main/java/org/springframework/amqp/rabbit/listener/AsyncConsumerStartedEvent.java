/*
 * Copyright 2016-present the original author or authors.
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
 * An event that is published whenever a new consumer is started.
 *
 * @author Gary Russell
 * @since 1.7
 *
 */
@SuppressWarnings("serial")
public class AsyncConsumerStartedEvent extends AmqpEvent {

	private final Object consumer;

	/**
	 * @param source the listener container.
	 * @param consumer the old consumer.
	 */
	public AsyncConsumerStartedEvent(Object source, Object consumer) {
		super(source);
		this.consumer = consumer;
	}

	public Object getConsumer() {
		return this.consumer;
	}

	@Override
	public String toString() {
		return "AsyncConsumerStartedEvent [consumer=" + this.consumer
				+ ", container=" + this.getSource() + "]";
	}

}
