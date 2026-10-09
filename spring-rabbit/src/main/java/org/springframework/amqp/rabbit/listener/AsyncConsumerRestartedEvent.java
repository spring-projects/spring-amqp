/*
 * Copyright 2016-present the original author or authors.
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
