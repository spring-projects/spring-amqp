/*
 * Copyright 2002-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.config.RabbitListenerConfigUtils;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * An {@link ImportBeanDefinitionRegistrar} class that registers
 * a {@link RabbitListenerAnnotationBeanPostProcessor} bean capable of processing
 * Spring's @{@link RabbitListener} annotation.
 * Also register a default {@link RabbitListenerEndpointRegistry}.
 *
 * <p>This configuration class is automatically imported when using the @{@link EnableRabbit}
 * annotation.
 *
 * @author Stephane Nicoll
 * @author Artem Bilan
 *
 * @since 1.4
 *
 * @see RabbitListenerAnnotationBeanPostProcessor
 * @see RabbitListenerEndpointRegistry
 * @see EnableRabbit
 */
public class RabbitBootstrapConfiguration implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(@Nullable AnnotationMetadata importingClassMetadata,
			BeanDefinitionRegistry registry) {

		if (!registry.containsBeanDefinition(
				RabbitListenerConfigUtils.RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)) {

			registry.registerBeanDefinition(RabbitListenerConfigUtils.RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME,
					new RootBeanDefinition(RabbitListenerAnnotationBeanPostProcessor.class));
		}

		if (!registry.containsBeanDefinition(RabbitListenerConfigUtils.RABBIT_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)) {
			registry.registerBeanDefinition(RabbitListenerConfigUtils.RABBIT_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
					new RootBeanDefinition(RabbitListenerEndpointRegistry.class));
		}
	}

}
