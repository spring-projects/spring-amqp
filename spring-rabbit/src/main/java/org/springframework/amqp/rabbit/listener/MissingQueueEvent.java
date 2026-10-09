/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import java.io.Serial;

import org.springframework.amqp.event.AmqpEvent;

/**
 * Event published when a missing queue is detected.
 *
 * @author Gary Russell
 * @since 2.1.18
 *
 */
public class MissingQueueEvent extends AmqpEvent {

	@Serial
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
