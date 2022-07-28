/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.amqp.rabbit;

import java.util.concurrent.CompletableFuture;

import org.springframework.amqp.core.AsyncAmqpTemplate2;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;

/**
 * This class was added in 2.4.7 to aid migration from methods returning
 * {@code ListenableFuture}s to {@link CompletableFuture}s.
 *
 * @author Gary Russell
 * @since 2.4.7
 * @deprecated in favor of {@link AsyncRabbitTemplate}.
 *
 */
@Deprecated
public class AsyncRabbitTemplate2 extends AsyncRabbitTemplate implements AsyncAmqpTemplate2 {

	public AsyncRabbitTemplate2(ConnectionFactory connectionFactory, String exchange, String routingKey,
			String replyQueue, String replyAddress) {
		super(connectionFactory, exchange, routingKey, replyQueue, replyAddress);
	}

	public AsyncRabbitTemplate2(ConnectionFactory connectionFactory, String exchange, String routingKey,
			String replyQueue) {
		super(connectionFactory, exchange, routingKey, replyQueue);
	}

	public AsyncRabbitTemplate2(ConnectionFactory connectionFactory, String exchange, String routingKey) {
		super(connectionFactory, exchange, routingKey);
	}

	public AsyncRabbitTemplate2(RabbitTemplate template, AbstractMessageListenerContainer container,
			String replyAddress) {
		super(template, container, replyAddress);
	}

	public AsyncRabbitTemplate2(RabbitTemplate template, AbstractMessageListenerContainer container) {
		super(template, container);
	}

	public AsyncRabbitTemplate2(RabbitTemplate template) {
		super(template);
	}

}
