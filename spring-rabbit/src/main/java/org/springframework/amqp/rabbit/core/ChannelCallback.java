/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import com.rabbitmq.client.Channel;

import org.springframework.lang.Nullable;

/**
 * Basic callback for use in RabbitTemplate.
 * @param <T> the type the callback returns.
 *
 * @author Mark Fisher
 * @author Gary Russell
 */
@FunctionalInterface
public interface ChannelCallback<T> {

	/**
	 * Execute any number of operations against the supplied RabbitMQ
	 * {@link Channel}, possibly returning a result.
	 *
	 * @param channel The channel.
	 * @return The result.
	 * @throws Exception Not sure what else Rabbit Throws
	 */
	@Nullable
	T doInRabbit(Channel channel) throws Exception; // NOSONAR user code might throw anything; cannot change

}
