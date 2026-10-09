/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.amqp.rabbit.test.context;

import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;

/**
 * The {@link ContextCustomizerFactory} implementation to produce a
 * {@link SpringRabbitContextCustomizer} if a {@link SpringRabbitTest} annotation
 * is present on the test class.
 *
 * @author Gary Russell
 *
 * @since 2.3
 *
 */
class SpringRabbitContextCustomizerFactory implements ContextCustomizerFactory {

	@Override
	public @Nullable ContextCustomizer createContextCustomizer(Class<?> testClass,
			List<ContextConfigurationAttributes> configAttributes) {
		SpringRabbitTest test =
				AnnotatedElementUtils.findMergedAnnotation(testClass, SpringRabbitTest.class);
		return test != null ? new SpringRabbitContextCustomizer(test) : null;
	}

}
