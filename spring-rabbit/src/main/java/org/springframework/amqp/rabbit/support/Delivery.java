/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;

/**
 * Encapsulates an arbitrary message - simple "bean" holder structure.
 *
 * @author Gary Russell
 * @since 2.0
 */
public class Delivery {

	private final String consumerTag;

	private final Envelope envelope;

	private final AMQP.BasicProperties properties;

	private final byte[] body;

	public Delivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) { //NOSONAR
		this.consumerTag = consumerTag;
		this.envelope = envelope;
		this.properties = properties;
		this.body = body;
	}

	/**
	 * Retrieve the consumer tag.
	 * @return the consumer tag.
	 */
	public String getConsumerTag() {
		return this.consumerTag;
	}

	/**
	 * Retrieve the message envelope.
	 * @return the message envelope.
	 */
	public Envelope getEnvelope() {
		return this.envelope;
	}

	/**
	 * Retrieve the message properties.
	 * @return the message properties.
	 */
	public BasicProperties getProperties() {
		return this.properties;
	}

	/**
	 * Retrieve the message body.
	 * @return the message body.
	 */
	public byte[] getBody() {
		return this.body;
	}

}
