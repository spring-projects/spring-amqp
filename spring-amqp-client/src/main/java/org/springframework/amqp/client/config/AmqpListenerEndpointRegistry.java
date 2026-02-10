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

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.client.listener.AmqpMessageListenerContainer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.Assert;

/**
 * An infrastructure bean to process {@link AmqpListenerEndpointRegistration} instances.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class AmqpListenerEndpointRegistry implements ApplicationContextAware {

	private final @Nullable AmqpMessageListenerContainerFactory defaultAmqpMessageListenerContainerFactory;

	@SuppressWarnings("NullAway.Init")
	private ConfigurableApplicationContext applicationContext;

	@SuppressWarnings("NullAway.Init")
	private DefaultListableBeanFactory beanFactory;

	public AmqpListenerEndpointRegistry() {
		this.defaultAmqpMessageListenerContainerFactory = null;
	}

	public AmqpListenerEndpointRegistry(AmqpMessageListenerContainerFactory amqpMessageListenerContainerFactory) {
		this.defaultAmqpMessageListenerContainerFactory = amqpMessageListenerContainerFactory;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		Assert.isInstanceOf(ConfigurableApplicationContext.class, applicationContext);
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		this.beanFactory = (DefaultListableBeanFactory) this.applicationContext.getBeanFactory();
	}

	/**
	 * Register {@link AmqpMessageListenerContainer} beans based on the provided {@link AmqpListenerEndpointRegistration}.
	 * @param registration the registration with endpoints to process.
	 * @return this registry.
	 */
	public AmqpListenerEndpointRegistry registerListenerEndpoints(AmqpListenerEndpointRegistration registration) {
		AmqpMessageListenerContainerFactory containerFactory = registration.getContainerFactory();
		if (containerFactory == null) {
			containerFactory = this.defaultAmqpMessageListenerContainerFactory;
		}

		Assert.state(containerFactory != null,
				"No 'AmqpMessageListenerContainerFactory' provided in the registration, " +
						"and no default listener container factory available.");

		for (AmqpListenerEndpoint endpoint : registration.getEndpoints()) {
			registerListenerEndpoint(containerFactory, endpoint);
		}

		return this;
	}

	/**
	 * Register {@link AmqpMessageListenerContainer} bean based on the provided {@link AmqpListenerEndpoint}.
	 * The default {@link AmqpMessageListenerContainerFactory} is required in this case.
	 * @param endpoint the endpoint to process from.
	 * @return this registry.
	 */
	public AmqpListenerEndpointRegistry registerListenerEndpoint(AmqpListenerEndpoint endpoint) {
		AmqpMessageListenerContainerFactory containerFactory = this.defaultAmqpMessageListenerContainerFactory;
		Assert.state(containerFactory != null, "No default listener container factory available.");
		registerListenerEndpoint(containerFactory, endpoint);

		return this;
	}

	/**
	 * Register {@link AmqpMessageListenerContainer} bean based on the provided {@link AmqpListenerEndpoint} and
	 * {@link AmqpMessageListenerContainerFactory}.
	 * @param containerFactory the factory to create a message listener container from.
	 * @param endpoint the endpoint to process from.
	 * @return this registry.
	 */
	public AmqpListenerEndpointRegistry registerListenerEndpoint(AmqpMessageListenerContainerFactory containerFactory,
			AmqpListenerEndpoint endpoint) {

		registerListenerContainer(containerFactory.createContainer(endpoint), endpoint);

		return this;
	}

	private void registerListenerContainer(AmqpMessageListenerContainer container, AmqpListenerEndpoint endpoint) {
		String id = endpoint.getId();
		if (id == null) {
			id = endpoint.getMessageListener().getClass().getName() + ".listenerContainer";
		}

		String generatedBeanName = id;
		int counter = -1;
		while (this.beanFactory.containsBean(id)) {
			id = generatedBeanName + BeanFactoryUtils.GENERATED_BEAN_NAME_SEPARATOR + counter++;
		}

		this.beanFactory.registerSingleton(id, container);
		this.beanFactory.initializeBean(container, id);
		if (this.applicationContext.isRunning() && container.isAutoStartup()) {
			container.start();
		}
	}

}
