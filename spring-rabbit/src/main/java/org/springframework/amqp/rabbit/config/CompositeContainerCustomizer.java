/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ContainerCustomizer} providing the configuration of
 * multiple customizers at the same time.
 *
 * @param <C> the container type.
 *
 * @author Rene Felgentraeger
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.4.8
 */
public class CompositeContainerCustomizer<C extends MessageListenerContainer> implements ContainerCustomizer<C> {

	private final List<ContainerCustomizer<C>> customizers;

	/**
	 * Create an instance with the provided delegate customizers.
	 * @param customizers the customizers.
	 */
	public CompositeContainerCustomizer(List<ContainerCustomizer<C>> customizers) {
		Assert.notNull(customizers, "At least one customizer must be present");
		this.customizers = new ArrayList<>(customizers);
	}

	@Override
	public void configure(C container) {
		this.customizers.forEach(c -> c.configure(container));
	}

}
