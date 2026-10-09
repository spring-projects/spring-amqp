/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.rabbit.annotation;

import org.springframework.context.annotation.DeferredImportSelector;
import org.springframework.core.annotation.Order;
import org.springframework.core.type.AnnotationMetadata;

/**
 * A {@link DeferredImportSelector} implementation with the lowest order to import a
 * {@link MultiRabbitBootstrapConfiguration} and {@link RabbitBootstrapConfiguration}
 * as late as possible.
 * {@link MultiRabbitBootstrapConfiguration} has precedence to be able to provide the
 * extended BeanPostProcessor, if enabled.
 *
 * @author Artem Bilan
 *
 * @since 2.1.6
 */
@Order
public class RabbitListenerConfigurationSelector implements DeferredImportSelector {

	@Override
	public String[] selectImports(AnnotationMetadata importingClassMetadata) {
		return new String[] { MultiRabbitBootstrapConfiguration.class.getName(),
				RabbitBootstrapConfiguration.class.getName()};
	}

}
