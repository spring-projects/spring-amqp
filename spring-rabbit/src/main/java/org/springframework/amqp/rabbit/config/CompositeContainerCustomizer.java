/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.amqp.rabbit.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ContainerCustomizer} providing the configuration of
 * multiple customizers at the same time.
 *
 * @param <C> the container type.
 *
 * @author Rene Felgentraeger
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.4.8
 */
public class CompositeContainerCustomizer<C extends MessageListenerContainer> implements ContainerCustomizer<C> {

	private final List<ContainerCustomizer<C>> customizers;

	/**
	 * Create an instance with the provided delegate customizers.
	 * @param customizers the customizers.
	 */
	public CompositeContainerCustomizer(List<ContainerCustomizer<C>> customizers) {
		Assert.notNull(customizers, "At least one customizer must be present");
		this.customizers = new ArrayList<>(customizers);
	}

	@Override
	public void configure(C container) {
		this.customizers.forEach(c -> c.configure(container));
	}

}
