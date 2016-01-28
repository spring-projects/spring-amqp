/*
 * Copyright 2014-2016 the original author or authors.
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

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.amqp.rabbit.listener.AbstractRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;

/**
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class RabbitListenerContainerTestFactory implements RabbitListenerContainerFactory<MessageListenerTestContainer> {

	private static final AtomicInteger counter = new AtomicInteger();

	private final Map<String, MessageListenerTestContainer> listenerContainers =
			new LinkedHashMap<String, MessageListenerTestContainer>();

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
		assertNotNull(this.getClass().getSimpleName() + " does not support " + endpoint.getClass().getSimpleName()
				+ " without an id", id);
		this.listenerContainers.put(id, container);
		return container;
	}

}
