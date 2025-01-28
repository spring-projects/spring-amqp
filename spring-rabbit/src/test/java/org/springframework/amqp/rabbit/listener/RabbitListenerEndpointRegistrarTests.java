/*
 * Copyright 2014-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * @author Stephane Nicoll
 * @author Gary Russell
 * @since 1.4
 */
public class RabbitListenerEndpointRegistrarTests {

	private final RabbitListenerEndpointRegistrar registrar = new RabbitListenerEndpointRegistrar();

	private final RabbitListenerEndpointRegistry registry = new RabbitListenerEndpointRegistry();

	private final RabbitListenerContainerTestFactory containerFactory = new RabbitListenerContainerTestFactory();


	@BeforeEach
	public void setup() {
		registrar.setEndpointRegistry(registry);
		registrar.setBeanFactory(new StaticListableBeanFactory());
	}

	@Test
	public void registerNullEndpoint() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> registrar.registerEndpoint(null, containerFactory));
	}

	@Test
	public void registerNullEndpointId() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> registrar.registerEndpoint(new SimpleRabbitListenerEndpoint(), containerFactory));
	}

	@Test
	public void registerEmptyEndpointId() {
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setId("");

		assertThatIllegalArgumentException()
			.isThrownBy(() -> registrar.registerEndpoint(endpoint, containerFactory));
	}

	@Test
	public void registerNullContainerFactoryIsAllowed() {
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setId("some id");
		registrar.setContainerFactory(containerFactory);
		registrar.registerEndpoint(endpoint, null);
		registrar.afterPropertiesSet();
		assertThat(registry.getListenerContainer("some id")).as("Container not created").isNotNull();
		assertThat(registry.getListenerContainerIds().iterator().next()).isEqualTo("some id");
		assertThat(registry.getListenerContainers()).hasSize(1);
	}

	@Test
	public void registerNullContainerFactoryWithNoDefault() {
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setId("some id");
		registrar.registerEndpoint(endpoint, null);

		assertThatIllegalStateException()
			.isThrownBy(() -> registrar.afterPropertiesSet())
			.withMessageContaining(endpoint.toString());
	}

	@Test
	public void registerContainerWithoutFactory() {
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setId("myEndpoint");
		registrar.setContainerFactory(containerFactory);
		registrar.registerEndpoint(endpoint);
		registrar.afterPropertiesSet();
		assertThat(registry.getListenerContainer("myEndpoint")).as("Container not created").isNotNull();
		assertThat(registry.getListenerContainerIds().iterator().next()).isEqualTo("myEndpoint");
		assertThat(registry.getListenerContainers()).hasSize(1);
	}

}
