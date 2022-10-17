package org.springframework.amqp.rabbit.config;

import java.util.List;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ContainerCustomizer<C>} providing the configuration of multiple customizers at the same time.
 */
public class CompositeContainerCustomizer<C extends MessageListenerContainer> implements ContainerCustomizer<C> {

	private final List<ContainerCustomizer<C>> customizers;

	public CompositeContainerCustomizer(List<ContainerCustomizer<C>> customizers) {
		Assert.notNull(customizers, "At least one customizer must be present");
		this.customizers = customizers;
	}

	@Override
	public void configure(C container) {
		customizers.forEach(c -> c.configure(container));
	}
}
