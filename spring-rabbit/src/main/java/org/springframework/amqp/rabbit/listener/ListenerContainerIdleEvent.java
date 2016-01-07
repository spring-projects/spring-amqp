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

import java.util.Date;

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

	private final long lastreceive;

	public ListenerContainerIdleEvent(SimpleMessageListenerContainer source, long lastReceive) {
		super(source);
		this.lastreceive = lastReceive;
	}

	/**
	 * When the last message was received
	 * @return the time.
	 */
	public long getLastreceive() {
		return lastreceive;
	}

	/**
	 * The queues the container is listening to.
	 * @return the queue names.
	 */
	public String[] getQueues() {
		return ((SimpleMessageListenerContainer) getSource()).getQueueNames();
	}

	/**
	 * The id of the listener, if the listener is a {@code @RabbitListener}.
	 * @return the id, or null.
	 */
	public String getListenerId() {
		return ((SimpleMessageListenerContainer) getSource()).getListenerId();
	}

	@Override
	public String toString() {
		return "ListenerContainerIdleEvent [lastreceive="
				+ new Date(lastreceive) + ", listenerId=" + getListenerId()
				+ ", container=" + getSource() + "]";
	}

}
