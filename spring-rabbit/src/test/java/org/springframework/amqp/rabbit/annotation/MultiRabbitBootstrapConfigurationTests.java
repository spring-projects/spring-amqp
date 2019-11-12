/*
 * Copyright 2002-2019 the original author or authors.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.config.RabbitListenerConfigUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;

/**
 * @author Wander Costa
 */
class MultiRabbitBootstrapConfigurationTests {

	private MultiRabbitBootstrapConfiguration configuration = new MultiRabbitBootstrapConfiguration();

	@Test
	@DisplayName("Test creation of MultiRabbitListenerAnnotationBeanPostProcessor bean")
	void testCreateBean() {
		BeanDefinitionRegistry registry = mock(BeanDefinitionRegistry.class);

		configuration.registerBeanDefinitions(null, registry);

		verify(registry).registerBeanDefinition(
				RabbitListenerConfigUtils.RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME,
				new RootBeanDefinition(MultiRabbitListenerAnnotationBeanPostProcessor.class));
	}

}
