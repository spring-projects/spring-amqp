/*
 * Copyright 2002-2024 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.lang.Nullable;

/**
 * Factory of {@link MessageListenerContainer}s.
 * @param <C> the container type.
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 1.4
 * @see RabbitListenerEndpoint
 */
public interface RabbitListenerContainerFactory<C extends MessageListenerContainer> extends BeanNameAware {

	/**
	 * Create a {@link MessageListenerContainer} for the given
	 * {@link RabbitListenerEndpoint}.
	 * @param endpoint the endpoint to configure.
	 * @return the created container.
	 */
	C createListenerContainer(@Nullable RabbitListenerEndpoint endpoint);

	/**
	 * Create a {@link MessageListenerContainer} with no
	 * {@link org.springframework.amqp.core.MessageListener} or queues; the listener must
	 * be added later before the container is started.
	 * @return the created container.
	 * @since 2.1.
	 */
	default C createListenerContainer() {
		return createListenerContainer(null);
	}

	@Override
	default void setBeanName(String name) {

	}

	/**
	 * Return a bean name of the component or null if not a bean.
	 * @return the bean name.
	 * @since 3.2
	 */
	@Nullable
	default String getBeanName() {
		return null;
	}

}
