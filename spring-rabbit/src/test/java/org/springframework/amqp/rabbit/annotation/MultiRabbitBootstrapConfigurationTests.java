/*
 * Copyright 2014-present the original author or authors.
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.springframework.amqp.rabbit.config.RabbitListenerConfigUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.env.Environment;

import static org.assertj.core.api.Assertions.assertThat;

class MultiRabbitBootstrapConfigurationTests {

	@Test
	@DisplayName("test if MultiRabbitBPP is registered when enabled")
	void testMultiRabbitBPPIsRegistered() throws Exception {
		final Environment environment = Mockito.mock(Environment.class);
		final ArgumentCaptor<RootBeanDefinition> captor = ArgumentCaptor.forClass(RootBeanDefinition.class);
		final BeanDefinitionRegistry registry = Mockito.mock(BeanDefinitionRegistry.class);
		final MultiRabbitBootstrapConfiguration bootstrapConfiguration = new MultiRabbitBootstrapConfiguration();
		bootstrapConfiguration.setEnvironment(environment);

		Mockito.when(environment.getProperty(RabbitListenerConfigUtils.MULTI_RABBIT_ENABLED_PROPERTY))
				.thenReturn("true");

		bootstrapConfiguration.registerBeanDefinitions(null, registry);

		Mockito.verify(registry).registerBeanDefinition(
				Mockito.eq(RabbitListenerConfigUtils.RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME),
				captor.capture());

		assertThat(captor.getValue().getBeanClass()).isEqualTo(MultiRabbitListenerAnnotationBeanPostProcessor.class);
	}

	@Test
	@DisplayName("test if MultiRabbitBPP is not registered when disabled")
	void testMultiRabbitBPPIsNotRegistered() throws Exception {
		final Environment environment = Mockito.mock(Environment.class);
		final BeanDefinitionRegistry registry = Mockito.mock(BeanDefinitionRegistry.class);
		final MultiRabbitBootstrapConfiguration bootstrapConfiguration = new MultiRabbitBootstrapConfiguration();
		bootstrapConfiguration.setEnvironment(environment);

		Mockito.when(environment.getProperty(RabbitListenerConfigUtils.MULTI_RABBIT_ENABLED_PROPERTY))
				.thenReturn("false");

		bootstrapConfiguration.registerBeanDefinitions(null, registry);

		Mockito.verify(registry, Mockito.never()).registerBeanDefinition(Mockito.anyString(),
				Mockito.any(RootBeanDefinition.class));
	}

}
