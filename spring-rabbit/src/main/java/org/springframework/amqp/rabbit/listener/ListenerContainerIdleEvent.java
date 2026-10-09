/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.event.AmqpEvent;

/**
 * An event that is emitted when a container is idle if the container
 * is configured to do so.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6
 *
 */
@SuppressWarnings("serial")
public class ListenerContainerIdleEvent extends AmqpEvent {

	private final long idleTime;

	private final @Nullable String listenerId;

	private final List<String> queueNames;

	public ListenerContainerIdleEvent(Object source, long idleTime, @Nullable String id, String... queueNames) {
		super(source);
		this.idleTime = idleTime;
		this.listenerId = id;
		this.queueNames = Arrays.asList(queueNames);
	}

	/**
	 * How long the container has been idle.
	 * @return the time in milliseconds.
	 */
	public long getIdleTime() {
		return this.idleTime;
	}

	/**
	 * The queues the container is listening to.
	 * @return the queue names.
	 */
	public String[] getQueueNames() {
		return this.queueNames.toArray(new String[0]);
	}

	/**
	 * The id of the listener (if {@code @RabbitListener}) or the container bean name.
	 * @return the id.
	 */
	@Nullable
	public String getListenerId() {
		return this.listenerId;
	}

	@Override
	public String toString() {
		return "ListenerContainerIdleEvent [idleTime="
				+ Duration.ofMillis(this.idleTime) + ", listenerId=" + this.listenerId
				+ ", container=" + getSource() + "]";
	}

}
