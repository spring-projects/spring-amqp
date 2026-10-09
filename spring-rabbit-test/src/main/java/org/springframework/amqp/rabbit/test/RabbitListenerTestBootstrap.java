/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.test;

import org.springframework.amqp.rabbit.config.RabbitListenerConfigUtils;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Overrides the default BPP with a {@link RabbitListenerTestHarness}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6
 *
 */
public class RabbitListenerTestBootstrap implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		registry.registerBeanDefinition(RabbitListenerConfigUtils.RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME,
				BeanDefinitionBuilder.rootBeanDefinition(RabbitListenerTestHarness.class)
						.addConstructorArgValue(importingClassMetadata)
						.getBeanDefinition());
	}

}
