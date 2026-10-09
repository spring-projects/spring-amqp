/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.rabbit.test;

import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurationSelector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.type.AnnotationMetadata;

/**
 * A {@link RabbitListenerConfigurationSelector} extension to register
 * a {@link RabbitListenerTestBootstrap}, but already with the higher order,
 * so the {@link RabbitListenerTestHarness} bean is registered earlier,
 * than {@link org.springframework.amqp.rabbit.annotation.RabbitListenerAnnotationBeanPostProcessor}.
 *
 * @author Artem Bilan
 *
 * @since 2.1.6
 */
@Order(Ordered.LOWEST_PRECEDENCE - 100) // NOSONAR magic
public class RabbitListenerTestSelector extends RabbitListenerConfigurationSelector {

	@Override
	public String[] selectImports(AnnotationMetadata importingClassMetadata) {
		return new String[] { RabbitListenerTestBootstrap.class.getName() };
	}

}
