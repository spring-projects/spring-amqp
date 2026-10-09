/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client.config;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.client.annotation.AmqpListenerAnnotationBeanPostProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

/**
 * The {@link Configuration} for AMQP 1.0 infrastructure to support
 * {@link org.springframework.amqp.client.annotation.AmqpListener}.
 * <p>
 * Registers {@link AmqpListenerEndpointRegistry} and {@link AmqpListenerAnnotationBeanPostProcessor}
 * with the {@link AmqpDefaultConfiguration#AMQP_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME}.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see EnableAmqp
 */
@Configuration(proxyBeanMethods = false)
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class AmqpDefaultConfiguration {

	/**
	 * The bean name of the default {@link AmqpMessageListenerContainerFactory}.
	 */
	public static final String AMQP_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME =
			"org.springframework.amqp.client.config.internalAmqpListenerAnnotationProcessor";

	/**
	 * The bean name of the default {@link AmqpMessageListenerContainerFactory}.
	 */
	public static final String DEFAULT_AMQP_LISTENER_CONTAINER_FACTORY_BEAN_NAME = "amqpListenerContainerFactory";

	@Bean(name = AMQP_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)
	static AmqpListenerAnnotationBeanPostProcessor amqpListenerAnnotationBeanPostProcessor() {
		return new AmqpListenerAnnotationBeanPostProcessor();
	}

	@Bean
	AmqpListenerEndpointRegistry amqpListenerEndpointRegistry(
			@Qualifier(DEFAULT_AMQP_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
			@Nullable AmqpMessageListenerContainerFactory factory) {

		return factory != null
				? new AmqpListenerEndpointRegistry(factory)
				: new AmqpListenerEndpointRegistry();
	}

}
