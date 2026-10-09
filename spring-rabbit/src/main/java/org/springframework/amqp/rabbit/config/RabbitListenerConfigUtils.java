/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

/**
 * Configuration constants for internal sharing across subpackages.
 *
 * @author Juergen Hoeller
 * @since 1.4
 */
public abstract class RabbitListenerConfigUtils {

	/**
	 * The bean name of the internally managed Rabbit listener annotation processor.
	 */
	public static final String RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME =
			"org.springframework.amqp.rabbit.config.internalRabbitListenerAnnotationProcessor";

	/**
	 * The bean name of the internally managed Rabbit listener endpoint registry.
	 */
	public static final String RABBIT_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME =
			"org.springframework.amqp.rabbit.config.internalRabbitListenerEndpointRegistry";

	/**
	 * The bean name of the default RabbitAdmin.
	 */
	public static final String RABBIT_ADMIN_BEAN_NAME = "amqpAdmin";

	/**
	 * The bean name of the default ConnectionFactory.
	 */
	public static final String RABBIT_CONNECTION_FACTORY_BEAN_NAME = "rabbitConnectionFactory";

	/**
	 * The default property to enable/disable MultiRabbit processing.
	 */
	public static final String MULTI_RABBIT_ENABLED_PROPERTY = "spring.multirabbitmq.enabled";

	/**
	 * The bean name of the ContainerFactory of the default broker for MultiRabbit.
	 */
	public static final String MULTI_RABBIT_CONTAINER_FACTORY_BEAN_NAME = "multiRabbitContainerFactory";

	/**
	 * The MultiRabbit admins' suffix.
	 */
	public static final String MULTI_RABBIT_ADMIN_SUFFIX = "-admin";

}
