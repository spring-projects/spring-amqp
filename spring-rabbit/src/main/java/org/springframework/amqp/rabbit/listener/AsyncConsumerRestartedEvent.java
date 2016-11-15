/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
 * An event that is published whenever a consumer is restarted.
 *
 * @author Gary Russell
 * @since 1.7
 *
 */
@SuppressWarnings("serial")
public class AsyncConsumerRestartedEvent extends AmqpEvent {

	private final Object oldConsumer;

	private final Object newConsumer;

	/**
	 * @param source the listener container.
	 * @param oldConsumer the old consumer.
	 * @param newConsumer the new consumer.
	 */
	public AsyncConsumerRestartedEvent(Object source, Object oldConsumer, Object newConsumer) {
		super(source);
		this.oldConsumer = oldConsumer;
		this.newConsumer = newConsumer;
	}

	public Object getOldConsumer() {
		return this.oldConsumer;
	}

	public Object getNewConsumer() {
		return this.newConsumer;
	}

	@Override
	public String toString() {
		return "AsyncConsumerRestartedEvent [oldConsumer=" + this.oldConsumer + ", newConsumer=" + this.newConsumer
				+ ", container=" + this.getSource() + "]";
	}

}
