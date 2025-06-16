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

package org.springframework.amqp.rabbit.config;

import org.springframework.amqp.rabbit.listener.MessageListenerContainer;

/**
 * Called by the container factory after the container is created and configured.
 *
 * @param <C> the container type.
 *
 * @author Gary Russell
 * @since 2.2.2
 *
 */
@FunctionalInterface
public interface ContainerCustomizer<C extends MessageListenerContainer> {

	/**
	 * Configure the container.
	 * @param container the container.
	 */
	void configure(C container);

}
