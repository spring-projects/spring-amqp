/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import org.springframework.amqp.event.AmqpEvent;

/**
 * An event that is published whenever a consumer is stopped (and not restarted).
 *
 * @author Gary Russell
 * @since 1.7
 *
 */
@SuppressWarnings("serial")
public class AsyncConsumerStoppedEvent extends AmqpEvent {

	private final Object consumer;

	/**
	 * @param source the listener container.
	 * @param consumer the old consumer.
	 */
	public AsyncConsumerStoppedEvent(Object source, Object consumer) {
		super(source);
		this.consumer = consumer;
	}

	public Object getConsumer() {
		return this.consumer;
	}

	@Override
	public String toString() {
		return "AsyncConsumerStoppedEvent [consumer=" + this.consumer
				+ ", container=" + this.getSource() + "]";
	}

}
