/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.amqp.rabbit.listener.AbstractRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class RabbitListenerContainerTestFactory implements RabbitListenerContainerFactory<MessageListenerTestContainer> {

	private static final AtomicInteger counter = new AtomicInteger();

	private final Map<String, MessageListenerTestContainer> listenerContainers =
			new LinkedHashMap<String, MessageListenerTestContainer>();

	private String beanName;

	public List<MessageListenerTestContainer> getListenerContainers() {
		return new ArrayList<MessageListenerTestContainer>(this.listenerContainers.values());
	}

	public MessageListenerTestContainer getListenerContainer(String id) {
		return this.listenerContainers.get(id);
	}

	@Override
	public MessageListenerTestContainer createListenerContainer(RabbitListenerEndpoint endpoint) {
		MessageListenerTestContainer container = new MessageListenerTestContainer(endpoint);

		// resolve the id
		if (endpoint.getId() == null && endpoint instanceof AbstractRabbitListenerEndpoint) {
			((AbstractRabbitListenerEndpoint) endpoint).setId("endpoint#" + counter.getAndIncrement());
		}
		String id = endpoint.getId();
		assertThat(id).as(this.getClass().getSimpleName() + " does not support " + endpoint.getClass().getSimpleName()
				+ " without an id").isNotNull();
		this.listenerContainers.put(id, container);
		return container;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	public String getBeanName() {
		return this.beanName;
	}

}
