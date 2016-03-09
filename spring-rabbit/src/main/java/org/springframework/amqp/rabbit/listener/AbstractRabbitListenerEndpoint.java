/*
 * Copyright 2014-2015 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.util.Assert;

/**
 * Base model for a Rabbit listener endpoint
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @since 1.4
 * @see MethodRabbitListenerEndpoint
 * @see SimpleRabbitListenerEndpoint
 */
public abstract class AbstractRabbitListenerEndpoint implements RabbitListenerEndpoint, BeanFactoryAware {

	private String id;

	private final Collection<Queue> queues = new ArrayList<Queue>();

	private final Collection<String> queueNames = new ArrayList<String>();

	private boolean exclusive;

	private Integer priority;

	private RabbitAdmin admin;

	private BeanFactory beanFactory;

	private BeanExpressionResolver resolver;

	private BeanExpressionContext expressionContext;

	private String group;


	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory, null);
		}
	}

	protected BeanFactory getBeanFactory() {
		return this.beanFactory;
	}

	protected BeanExpressionResolver getResolver() {
		return this.resolver;
	}

	protected BeanExpressionContext getBeanExpressionContext() {
		return this.expressionContext;
	}

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
	 * @param queues to set.
	 * @see #setQueueNames
	 */
	public void setQueues(Queue... queues) {
		Assert.notNull(queues, "'queues' must not be null");
		this.queues.clear();
		this.queues.addAll(Arrays.asList(queues));
	}

	/**
	 * @return the queues for this endpoint.
	 */
	public Collection<Queue> getQueues() {
		return this.queues;
	}

	/**
	 * @return the queue names for this endpoint.
	 */
	public Collection<String> getQueueNames() {
		return this.queueNames;
	}

	/**
	 * Set the queue names to use. Either the {@link Queue} instances or the
	 * queue names should be provided, but not both.
	 * @param queueNames to set.
	 * @see #setQueues
	 */
	public void setQueueNames(String... queueNames) {
		Assert.notNull(queueNames, "'queueNames' must not be null");
		this.queueNames.clear();
		this.queueNames.addAll(Arrays.asList(queueNames));
	}

	/**
	 * Set if a single consumer in the container will have exclusive use of the
	 * queues, preventing other consumers from receiving messages from the
	 * queue(s).
	 * @param exclusive the exclusive {@code boolean} flag.
	 */
	public void setExclusive(boolean exclusive) {
		this.exclusive = exclusive;
	}

	/**
	 * @return the exclusive {@code boolean} flag.
	 */
	public boolean isExclusive() {
		return this.exclusive;
	}

	/**
	 * Set the priority of this endpoint.
	 * @param priority the priority value.
	 */
	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	/**
	 * @return the priority of this endpoint or {@code null} if
	 * no priority is set.
	 */
	public Integer getPriority() {
		return this.priority;
	}

	/**
	 * Set the {@link RabbitAdmin} instance to use.
	 * @param admin the {@link RabbitAdmin} instance.
	 */
	public void setAdmin(RabbitAdmin admin) {
		this.admin = admin;
	}

	/**
	 * @return the {@link RabbitAdmin} instance to use or {@code null} if
	 * none is configured.
	 */
	public RabbitAdmin getAdmin() {
		return this.admin;
	}

	@Override
	public String getGroup() {
		return this.group;
	}

	/**
	 * Set the group for the corresponding listener container.
	 * @param group the group.
	 * @since 1.5
	 */
	public void setGroup(String group) {
		this.group = group;
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
	 * @param container the {@link MessageListenerContainer} to create a {@link MessageListener}.
	 * @return a a {@link MessageListener} instance.
	 */
	protected abstract MessageListener createMessageListener(MessageListenerContainer container);

	private void setupMessageListener(MessageListenerContainer container) {
		MessageListener messageListener = createMessageListener(container);
		Assert.state(messageListener != null, "Endpoint [" + this + "] must provide a non null message listener");
		container.setupMessageListener(messageListener);
	}

	/**
	 * @return a description for this endpoint.
	 * <p>Available to subclasses, for inclusion in their {@code toString()} result.
	 */
	protected StringBuilder getEndpointDescription() {
		StringBuilder result = new StringBuilder();
		return result.append(getClass().getSimpleName()).append("[").append(this.id).
				append("] queues=").append(this.queues).
				append("' | queueNames='").append(this.queueNames).
				append("' | exclusive='").append(this.exclusive).
				append("' | priority='").append(this.priority).
				append("' | admin='").append(this.admin).append("'");
	}

	@Override
	public String toString() {
		return getEndpointDescription().toString();
	}

}
