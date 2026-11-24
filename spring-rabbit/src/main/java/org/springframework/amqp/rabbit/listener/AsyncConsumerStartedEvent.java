/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
