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

import org.springframework.amqp.rabbit.connection.SimpleResourceHolder;

/**
 * An {@link AbstractRoutingAmqpConnectionFactory} implementation which gets a {@code lookupKey}
 * for the current {@link AmqpConnectionFactory} from a thread-bound resource by the key of the
 * instance of this {@link AmqpConnectionFactory}.
 *
 * @author Robin Collard
 *
 * @since 4.1.1
 *
 * @see SimpleResourceHolder
 */
public class SimpleRoutingAmqpConnectionFactory extends AbstractRoutingAmqpConnectionFactory {

	@Override
	protected @Nullable Object determineCurrentLookupKey() {
		return SimpleResourceHolder.get(this);
	}

}
