/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.annotation;

import org.springframework.amqp.rabbit.config.RabbitListenerConfigUtils;
import org.springframework.amqp.rabbit.config.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

/**
 * {@code @Configuration} class that registers a {@link RabbitListenerAnnotationBeanPostProcessor}
 * bean capable of processing Spring's @{@link RabbitListener} annotation. Also register
 * a default {@link RabbitListenerEndpointRegistry}.
 *
 * <p>This configuration class is automatically imported when using the @{@link EnableRabbit}
 * annotation.  See {@link EnableRabbit} Javadoc for complete usage.
 *
 * @author Stephane Nicoll
 * @since 2.0
 * @see RabbitListenerAnnotationBeanPostProcessor
 * @see RabbitListenerEndpointRegistry
 * @see EnableRabbit
 */
@Configuration
public class RabbitBootstrapConfiguration {

	@Bean(name = RabbitListenerConfigUtils.RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public RabbitListenerAnnotationBeanPostProcessor rabbitListenerAnnotationProcessor() {
		return new RabbitListenerAnnotationBeanPostProcessor();
	}

	@Bean(name = RabbitListenerConfigUtils.RABBIT_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)
	public RabbitListenerEndpointRegistry defaultRabbitListenerEndpointRegistry() {
		return new RabbitListenerEndpointRegistry();
	}

}
