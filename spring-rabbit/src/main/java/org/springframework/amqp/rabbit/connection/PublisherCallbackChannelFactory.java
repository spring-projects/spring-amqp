/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import java.util.concurrent.ExecutorService;

import com.rabbitmq.client.Channel;

/**
 * A factory for {@link PublisherCallbackChannel}s.
 *
 * @author Gary Russell
 * @since 2.1.6
 *
 */
@FunctionalInterface
public interface PublisherCallbackChannelFactory {

	/**
	 * Create a {@link PublisherCallbackChannel} instance based on the provided delegate
	 * and executor.
	 * @param delegate the delegate channel.
	 * @param executor the executor.
	 * @return the channel.
	 */
	PublisherCallbackChannel createChannel(Channel delegate, ExecutorService executor);

}
