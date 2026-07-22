/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.rabbitmq.client;

import org.jspecify.annotations.Nullable;

/**
 * Implementations select an {@link AmqpConnectionFactory} based on a supplied key.
 *
 * @author Robin Collard
 *
 * @since 4.1.1
 */
@FunctionalInterface
public interface RoutingAmqpConnectionFactory {

	/**
	 * Returns the {@link AmqpConnectionFactory} bound to given lookup key, or {@code null}
	 * if one does not exist.
	 * @param key the lookup key to which the {@link AmqpConnectionFactory} is bound.
	 * @return the {@link AmqpConnectionFactory} bound to the given lookup key,
	 * or {@code null} if one does not exist.
	 */
	@Nullable
	AmqpConnectionFactory getTargetConnectionFactory(Object key);

}
