/*
 * Copyright 2014-present the original author or authors.
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
