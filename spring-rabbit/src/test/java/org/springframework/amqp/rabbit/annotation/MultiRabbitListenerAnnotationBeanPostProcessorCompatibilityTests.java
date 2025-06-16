/*
 * Copyright 2020-present the original author or authors.
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
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * This test is an extension of {@link RabbitListenerAnnotationBeanPostProcessorTests}
 * in order to guarantee the compatibility of MultiRabbit with previous use cases. This
 * ensures that multirabbit does not break single rabbit applications.
 *
 * @author Wander Costa
 */
class MultiRabbitListenerAnnotationBeanPostProcessorCompatibilityTests
		extends RabbitListenerAnnotationBeanPostProcessorTests {

	@Override
	protected Class<?> getConfigClass() {
		return MultiConfig.class;
	}

	@Configuration
	@PropertySource("classpath:/org/springframework/amqp/rabbit/annotation/queue-annotation.properties")
	static class MultiConfig extends RabbitListenerAnnotationBeanPostProcessorTests.Config {

		@Bean
		@Override
		public MultiRabbitListenerAnnotationBeanPostProcessor postProcessor() {
			MultiRabbitListenerAnnotationBeanPostProcessor postProcessor
					= new MultiRabbitListenerAnnotationBeanPostProcessor();
			postProcessor.setEndpointRegistry(rabbitListenerEndpointRegistry());
			postProcessor.setContainerFactoryBeanName("testFactory");
			return postProcessor;
		}

		@Bean(RabbitListenerConfigUtils.RABBIT_ADMIN_BEAN_NAME)
		public RabbitAdmin defaultRabbitAdmin() {
			return new RabbitAdmin(new SingleConnectionFactory());
		}
	}
}
