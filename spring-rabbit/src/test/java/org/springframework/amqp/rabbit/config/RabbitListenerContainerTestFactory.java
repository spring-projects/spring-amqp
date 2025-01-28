/*
 * Copyright 2014-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
