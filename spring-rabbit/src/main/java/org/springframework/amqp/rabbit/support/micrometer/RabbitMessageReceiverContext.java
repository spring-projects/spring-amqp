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

package org.springframework.amqp.rabbit.support.micrometer;

import io.micrometer.observation.transport.ReceiverContext;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;

/**
 * {@link ReceiverContext} for {@link Message}s.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 3.0
 *
 */
public class RabbitMessageReceiverContext extends ReceiverContext<Message> {

	private final String listenerId;

	private final Message message;

	@SuppressWarnings("this-escape")
	public RabbitMessageReceiverContext(Message message, String listenerId) {
		super((carrier, key) -> carrier.getMessageProperties().getHeader(key));
		setCarrier(message);
		this.message = message;
		this.listenerId = listenerId;
		setRemoteServiceName("RabbitMQ");
	}

	@Override
	public Message getCarrier() {
		return this.message;
	}

	public String getListenerId() {
		return this.listenerId;
	}

	/**
	 * Return the source (queue) for this message.
	 * @return the source.
	 */
	public @Nullable String getSource() {
		return this.message.getMessageProperties().getConsumerQueue();
	}

}
