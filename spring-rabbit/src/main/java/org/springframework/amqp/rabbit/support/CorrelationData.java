/*
 * Copyright 2002-2018 the original author or authors.
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

import org.springframework.amqp.core.Correlation;

/**
 * Base class for correlating publisher confirms to sent messages.
 * Use the {@link org.springframework.amqp.rabbit.core.RabbitTemplate}
 * methods that include one of
 * these as a parameter; when the publisher confirm is received,
 * the CorrelationData is returned with the ack/nack.
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public class CorrelationData implements Correlation {

	private volatile String id;

	/**
	 * Construct an instance with a null Id.
	 * @since 1.6.7
	 */
	public CorrelationData() {
		super();
	}

	/**
	 * Construct an instance with the supplied id.
	 * @param id the id.
	 */
	public CorrelationData(String id) {
		this.id = id;
	}

	public String getId() {
		return this.id;
	}

	/**
	 * Set the correlation id. Generally, the correlation id shouldn't be changed.
	 * One use case, however, is when it needs to be set in a
	 * {@link org.springframework.amqp.core.MessagePostProcessor} after a
	 * {@link CorrelationData} with a 'null' correlation id has been passed into a
	 * {@link org.springframework.amqp.rabbit.core.RabbitTemplate}.
	 *
	 * @param id the id.
	 * @since 1.6
	 */
	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "CorrelationData [id=" + this.id + "]";
	}

}
