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

import org.springframework.amqp.rabbit.config.RabbitListenerConfigUtils;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * An {@link ImportBeanDefinitionRegistrar} class that registers a
 * {@link MultiRabbitListenerAnnotationBeanPostProcessor} (overwriting the regular
 * {@link RabbitListenerAnnotationBeanPostProcessor}) bean capable of processing
 * Spring's @{@link RabbitListener} annotations.
 * <p>
 * This configuration must be imported manually.
 *
 * @author Wander Costa
 * @see MultiRabbitListenerAnnotationBeanPostProcessor
 * @see RabbitListenerAnnotationBeanPostProcessor
 * @see RabbitListenerEndpointRegistry
 * @see EnableRabbit
 */
public class MultiRabbitBootstrapConfiguration implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		registry.registerBeanDefinition(
				RabbitListenerConfigUtils.RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME,
				new RootBeanDefinition(MultiRabbitListenerAnnotationBeanPostProcessor.class));
	}

}
