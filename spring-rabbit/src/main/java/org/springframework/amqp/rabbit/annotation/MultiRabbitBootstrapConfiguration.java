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

import org.springframework.amqp.rabbit.config.RabbitListenerConfigUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.lang.Nullable;

/**
 * An {@link ImportBeanDefinitionRegistrar} class that registers
 * a {@link MultiRabbitListenerAnnotationBeanPostProcessor} bean, if MultiRabbit
 * is enabled.
 *
 * @author Wander Costa
 *
 * @since 1.4
 *
 * @see RabbitListenerAnnotationBeanPostProcessor
 * @see MultiRabbitListenerAnnotationBeanPostProcessor
 * @see org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry
 * @see EnableRabbit
 */
public class MultiRabbitBootstrapConfiguration implements ImportBeanDefinitionRegistrar, EnvironmentAware {

	private Environment environment;

	@Override
	public void registerBeanDefinitions(@Nullable AnnotationMetadata importingClassMetadata,
			BeanDefinitionRegistry registry) {

		if (isMultiRabbitEnabled() && !registry.containsBeanDefinition(
				RabbitListenerConfigUtils.RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)) {

			registry.registerBeanDefinition(RabbitListenerConfigUtils.RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME,
					new RootBeanDefinition(MultiRabbitListenerAnnotationBeanPostProcessor.class));
		}
	}

	private boolean isMultiRabbitEnabled() {
		final String isMultiEnabledStr =  this.environment.getProperty(
				RabbitListenerConfigUtils.MULTI_RABBIT_ENABLED_PROPERTY);
		return Boolean.parseBoolean(isMultiEnabledStr);
	}

	@Override
	public void setEnvironment(final Environment environment) {
		this.environment = environment;
	}
}
