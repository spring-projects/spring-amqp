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

package org.springframework.amqp.rabbit.annotation;


import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;

/**
 * Optional interface to be implemented by Spring managed bean willing
 * to customize how Rabbit listener endpoints are configured. Typically
 * used to defined the default
 * {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory
 * RabbitListenerContainerFactory} to use or for registering Rabbit endpoints
 * in a <em>programmatic</em> fashion as opposed to the <em>declarative</em>
 * approach of using the @{@link RabbitListener} annotation.
 *
 * <p>See @{@link EnableRabbit} for detailed usage examples.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @since 1.4
 * @see EnableRabbit
 * @see org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar
 */
@FunctionalInterface
public interface RabbitListenerConfigurer {

	/**
	 * Callback allowing a {@link org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry
	 * RabbitListenerEndpointRegistry} and specific {@link org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint
	 * RabbitListenerEndpoint} instances to be registered against the given
	 * {@link RabbitListenerEndpointRegistrar}. The default
	 * {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory RabbitListenerContainerFactory}
	 * can also be customized.
	 * @param registrar the registrar to be configured
	 */
	void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar);

}
