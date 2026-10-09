/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import org.springframework.amqp.core.MessageListenerContainer;

/**
 * Called by the container factory after the container is created and configured.
 *
 * @param <C> the container type.
 *
 * @author Gary Russell
 * @since 2.2.2
 *
 */
@FunctionalInterface
public interface ContainerCustomizer<C extends MessageListenerContainer> {

	/**
	 * Configure the container.
	 * @param container the container.
	 */
	void configure(C container);

}
