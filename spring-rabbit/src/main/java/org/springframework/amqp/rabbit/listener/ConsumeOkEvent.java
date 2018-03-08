/*
 * Copyright 2017-2018 the original author or authors.
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
 * An {@link AmqpEvent} emitted by the listener container
 * when consumer is subscribed to the queue.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.7.5
 *
 */
@SuppressWarnings("serial")
public class ConsumeOkEvent extends AmqpEvent {

	private final String queue;

	private final String consumerTag;

	/**
	 * Instantiate a {@link ConsumeOkEvent} based on the provided
	 * consumer, queue and consumer tag.
	 * @param source the consumer subscribed to the queue
	 * @param queue the queue to consume
	 * @param consumerTag the tag indicate a consumer subscription
	 */
	public ConsumeOkEvent(Object source, String queue, String consumerTag) {
		super(source);
		this.queue = queue;
		this.consumerTag = consumerTag;
	}

	/**
	 * Obtain the queue name a consumer has been subscribed.
	 * @return the queue name a consumer subscribed.
	 * @since 1.7.7
	 */
	public String getQueue() {
		return this.queue;
	}

	/**
	 * Obtain the consumer tag assigned to the consumer.
	 * @return the consumer tag for subscription.
	 * @since 1.7.7
	 */
	public String getConsumerTag() {
		return this.consumerTag;
	}

	@Override
	public String toString() {
		return "ConsumeOkEvent [queue=" + this.queue + ", consumerTag=" + this.consumerTag
				+ ", consumer=" + getSource() + "]";
	}

}
