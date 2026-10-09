/*
 * Copyright 2020-present the original author or authors.
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
