/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.listener;

import java.util.Map;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.event.AmqpEvent;
import org.springframework.util.Assert;

/**
 * @author Gary Russell
 * @since 2.1
 *
 */
@SuppressWarnings("serial")
public class BrokerEvent extends AmqpEvent {

	private final MessageProperties properties;

	public BrokerEvent(Object source, MessageProperties properties) {
		super(source);
		Assert.notNull(properties, "MessageProperties cannot be null");
		this.properties = properties;
	}

	public String getEventType() {
		return this.properties.getReceivedRoutingKey();
	}

	public Map<String, Object> getHeaders() {
		return this.properties.getHeaders();
	}

	public MessageProperties getProperties() {
		return this.properties;
	}

	@Override
	public String toString() {
		return "BrokerEvent [eventType=" + this.getEventType() + ", headers=" + this.getHeaders() + "]";
	}

}
