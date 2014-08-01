/*
 * Copyright 2002-2014 the original author or authors.
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

package org.springframework.amqp.rabbit.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.AbstractAdaptableMessageListener;
import org.springframework.util.Assert;

/**
 * Base model for a Rabbit listener endpoint
 *
 * @author Stephane Nicoll
 * @since 2.0
 * @see MethodRabbitListenerEndpoint
 * @see SimpleRabbitListenerEndpoint
 */
public abstract class AbstractRabbitListenerEndpoint implements RabbitListenerEndpoint {

	private String id;

	private Collection<Queue> queues = new ArrayList<Queue>();

	private Collection<String> queueNames = new ArrayList<String>();

	private boolean exclusive;

	private Integer priority;

	private String responseRoutingKey;

	private RabbitAdmin admin;


	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return this.id;
	}

	/**
	 * Set the queues to use. Either the {@link Queue} instances or the
	 * queue names should be provided, but not both.
	 * @see #setQueueNames
	 */
	public void setQueues(Queue... queues) {
		Assert.notNull(queues, "Queues must not be null");
		this.queues.clear();
		this.queues.addAll(Arrays.asList(queues));
	}

	/**
	 * Return the queues for this endpoint.
	 */
	public Collection<Queue> getQueues() {
		return queues;
	}

	/**
	 * Set the queue names to use. Either the {@link Queue} instances or the
	 * queue names should be provided, but not both.
	 * @see #setQueues
	 */
	public Collection<String> getQueueNames() {
		return queueNames;
	}

	/**
	 * Return the queue names for this endpoint.
	 */
	public void setQueueNames(String... queueNames) {
		this.queueNames.clear();
		this.queueNames.addAll(Arrays.asList(queueNames));
	}

	/**
	 * Set if a single consumer in the container will have exclusive use of the
	 * queues, preventing other consumers from receiving messages from the
	 * queue(s).
	 */
	public void setExclusive(boolean exclusive) {
		this.exclusive = exclusive;
	}

	/**
	 * Specify if a single consumer in the container will have exclusive use of the
	 * queues.
	 */
	public boolean isExclusive() {
		return exclusive;
	}

	/**
	 * Set the priority of this endpoint
	 */
	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	/**
	 * Return the priority of this endpoint or {@code null} if
	 * no priority is set.
	 */
	public Integer getPriority() {
		return priority;
	}

	/**
	 * Set the routing key to send along with a response message.
	 */
	public void setResponseRoutingKey(String responseRoutingKey) {
		this.responseRoutingKey = responseRoutingKey;
	}

	/**
	 * Return the routing key to send along with a response message.
	 */
	public String getResponseRoutingKey() {
		return responseRoutingKey;
	}

	/**
	 * Set the {@link RabbitAdmin} instance to use.
	 */
	public void setAdmin(RabbitAdmin admin) {
		this.admin = admin;
	}

	/**
	 * Return the {@link RabbitAdmin} instance to use or {@code null} if
	 * none is configured.
	 */
	public RabbitAdmin getAdmin() {
		return admin;
	}

	@Override
	public void setupListenerContainer(MessageListenerContainer listenerContainer) {
		SimpleMessageListenerContainer container = (SimpleMessageListenerContainer) listenerContainer;

		boolean queuesEmpty = getQueues().isEmpty();
		boolean queueNamesEmpty = getQueueNames().isEmpty();
		if (!queuesEmpty && !queueNamesEmpty) {
			throw new IllegalStateException("Queues or queue names must be provided but not both for " + this);
		}
		if (queuesEmpty) {
			Collection<String> names = getQueueNames();
			container.setQueueNames(names.toArray(new String[names.size()]));
		}
		else {
			Collection<Queue> instances = getQueues();
			container.setQueues(instances.toArray(new Queue[instances.size()]));
		}

		container.setExclusive(isExclusive());
		if (getPriority() != null) {
			Map<String, Object> args = new HashMap<String, Object>();
			args.put("x-priority", getPriority());
			container.setConsumerArguments(args);
		}

		if (getAdmin() != null) {
			container.setRabbitAdmin(getAdmin());
		}
		setupMessageListener(listenerContainer);
	}

	/**
	 * Create a {@link MessageListener} that is able to serve this endpoint for the
	 * specified container.
	 */
	protected abstract MessageListener createMessageListener(MessageListenerContainer container);

	private void setupMessageListener(MessageListenerContainer container) {
		MessageListener messageListener = createMessageListener(container);
		if (getResponseRoutingKey() != null && messageListener instanceof AbstractAdaptableMessageListener) {
			((AbstractAdaptableMessageListener) messageListener).setResponseRoutingKey(getResponseRoutingKey());
		}
		Assert.state(messageListener != null, "Endpoint [" + this + "] must provide a non null message listener");
		container.setupMessageListener(messageListener);
	}

	/**
	 * Return a description for this endpoint.
	 * <p>Available to subclasses, for inclusion in their {@code toString()} result.
	 */
	protected StringBuilder getEndpointDescription() {
		StringBuilder result = new StringBuilder();
		return result.append(getClass().getSimpleName()).append("[").append(this.id).
				append("] queues=").append(this.queues).
				append("' | queueNames='").append(this.queueNames).
				append("' | exclusive='").append(this.exclusive).
				append("' | priority='").append(this.priority).
				append("' | responseRoutingKey='").append(this.responseRoutingKey).
				append("' | admin='").append(this.admin).append("'");
	}

	@Override
	public String toString() {
		return getEndpointDescription().toString();
	}

}
