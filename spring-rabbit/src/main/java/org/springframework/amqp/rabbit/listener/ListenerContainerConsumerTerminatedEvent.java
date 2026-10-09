/*
 * Copyright 2015-present the original author or authors.
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
