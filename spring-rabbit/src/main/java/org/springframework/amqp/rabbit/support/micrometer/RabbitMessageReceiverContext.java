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

package org.springframework.amqp.rabbit.support.micrometer;

import org.springframework.amqp.core.Message;

import io.micrometer.observation.transport.ReceiverContext;

/**
 * {@link ReceiverContext} for {@link Message}s.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public class RabbitMessageReceiverContext extends ReceiverContext<Message> {

	private final String listenerId;

	private final String queue;

	public RabbitMessageReceiverContext(Message message, String listenerId, String queue) {
		super((carrier, key) -> carrier.getMessageProperties().getHeader(key));
		setCarrier(message);
		this.listenerId = listenerId;
		this.queue = queue;
	}

	public String getListenerId() {
		return this.listenerId;
	}

	@Override
	public String getContextualName() {
		return queue + " receive";
	}

}
