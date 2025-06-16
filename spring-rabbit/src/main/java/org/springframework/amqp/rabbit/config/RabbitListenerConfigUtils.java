/*
 * Copyright 2002-present the original author or authors.
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
