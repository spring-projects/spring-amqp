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

import java.util.Arrays;
import java.util.List;

import org.springframework.amqp.event.AmqpEvent;

/**
 * An event that is emitted when a container is idle if the container
 * is configured to do so.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
@SuppressWarnings("serial")
public class ListenerContainerIdleEvent extends AmqpEvent {

	private final long idleTime;

	private final String listenerId;

	private final List<String> queueNames;

	public ListenerContainerIdleEvent(Object source, long idleTime, String id, String... queueNames) {
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
		return this.queueNames.toArray(new String[this.queueNames.size()]);
	}

	/**
	 * The id of the listener (if {@code @RabbitListener}) or the container bean name.
	 * @return the id.
	 */
	public String getListenerId() {
		return this.listenerId;
	}

	@Override
	public String toString() {
		return "ListenerContainerIdleEvent [idleTime="
				+ ((float) this.idleTime / 1000) + "s, listenerId=" + this.listenerId
				+ ", container=" + getSource() + "]";
	}

}
