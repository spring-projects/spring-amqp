/*
 * Copyright 2021-present the original author or authors.
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

package org.springframework.amqp.rabbitmq.client.listener;

import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Message;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageListener;

/**
 * A message listener that receives native AMQP 1.0 messages from RabbitMQ.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public interface RabbitAmqpMessageListener extends MessageListener {

	/**
	 * Process an AMQP message.
	 * @param message the message to process.
	 * @param context the consumer context to settle message.
	 *                Null if container is configured for {@code autoSettle}.
	 */
	void onAmqpMessage(Message message, Consumer.@Nullable Context context);

}
