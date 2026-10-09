/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanNameAware;

/**
 * Factory of {@link MessageListenerContainer}s.
 * @param <C> the container type.
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 1.4
 * @see RabbitListenerEndpoint
 */
@FunctionalInterface
public interface RabbitListenerContainerFactory<C extends MessageListenerContainer> extends BeanNameAware {

	/**
	 * Create a {@link MessageListenerContainer} for the given
	 * {@link RabbitListenerEndpoint}.
	 * @param endpoint the endpoint to configure.
	 * @return the created container.
	 */
	C createListenerContainer(@Nullable RabbitListenerEndpoint endpoint);

	/**
	 * Create a {@link MessageListenerContainer} with no
	 * {@link org.springframework.amqp.core.MessageListener} or queues; the listener must
	 * be added later before the container is started.
	 * @return the created container.
	 * @since 2.1.
	 */
	default C createListenerContainer() {
		return createListenerContainer(null);
	}

	@Override
	default void setBeanName(String name) {

	}

	/**
	 * Return a bean name of the component or null if not a bean.
	 * @return the bean name.
	 * @since 3.2
	 */
	@Nullable
	default String getBeanName() {
		return null;
	}

}
