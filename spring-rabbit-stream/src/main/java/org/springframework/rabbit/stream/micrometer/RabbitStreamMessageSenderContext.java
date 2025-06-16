/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.rabbit.stream.micrometer;

import java.util.Map;

import com.rabbitmq.stream.Message;
import io.micrometer.observation.transport.SenderContext;

/**
 * {@link SenderContext} for {@link Message}s.
 *
 * @author Gary Russell
 * @since 3.0.5
 *
 */
public class RabbitStreamMessageSenderContext extends SenderContext<Message> {

	private final String beanName;

	private final String destination;

	@SuppressWarnings("this-escape")
	public RabbitStreamMessageSenderContext(Message message, String beanName, String destination) {
		super((carrier, key, value) -> {
			Map<String, Object> props = message.getApplicationProperties();
			if (props != null) {
				props.put(key, value);
			}
		});
		setCarrier(message);
		this.beanName = beanName;
		this.destination = destination;
		setRemoteServiceName("RabbitMQ Stream");
	}

	public String getBeanName() {
		return this.beanName;
	}

	/**
	 * Return the destination - {@code exchange/routingKey}.
	 * @return the destination.
	 */
	public String getDestination() {
		return this.destination;
	}

}
