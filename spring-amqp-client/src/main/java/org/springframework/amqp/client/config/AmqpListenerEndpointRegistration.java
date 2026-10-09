/*
 * Copyright 2026-present the original author or authors.
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
