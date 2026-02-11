/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * The configuration container to trigger {@link org.springframework.amqp.client.listener.AmqpMessageListenerContainer}
 * bean registrations based on the provided {@link AmqpListenerEndpoint} instances
 * and {@link AmqpMessageListenerContainerFactory}.
 * <p>
 * If {@link AmqpMessageListenerContainerFactory} is not provided,
 * a bean with name {@link AmqpDefaultConfiguration#DEFAULT_AMQP_LISTENER_CONTAINER_FACTORY_BEAN_NAME}
 * is used by default.
 * <p>
 * The instance of this class can be declared as a bean, or used directly with the {@link AmqpListenerEndpointRegistry}.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see AmqpListenerEndpointRegistry
 */
public class AmqpListenerEndpointRegistration {

	private final List<AmqpListenerEndpoint> endpoints = new ArrayList<>();

	private final @Nullable AmqpMessageListenerContainerFactory factory;

	public AmqpListenerEndpointRegistration(AmqpListenerEndpoint... endpoints) {
		this.factory = null;
		this.endpoints.addAll(List.of(endpoints));
	}

	public AmqpListenerEndpointRegistration(AmqpMessageListenerContainerFactory factory,
			AmqpListenerEndpoint... endpoints) {

		this.factory = factory;
		this.endpoints.addAll(List.of(endpoints));
	}

	public AmqpListenerEndpointRegistration addEndpoint(AmqpListenerEndpoint endpoint) {
		this.endpoints.add(endpoint);
		return this;
	}

	public List<AmqpListenerEndpoint> getEndpoints() {
		return Collections.unmodifiableList(this.endpoints);
	}

	public @Nullable AmqpMessageListenerContainerFactory getContainerFactory() {
		return this.factory;
	}

}
