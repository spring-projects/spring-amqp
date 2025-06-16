/*
 * Copyright 2019-present the original author or authors.
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
