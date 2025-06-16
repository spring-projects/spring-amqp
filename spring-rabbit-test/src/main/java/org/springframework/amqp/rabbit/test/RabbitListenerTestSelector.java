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
