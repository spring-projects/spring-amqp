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
