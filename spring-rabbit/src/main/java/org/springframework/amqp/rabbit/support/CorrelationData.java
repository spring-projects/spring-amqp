/*
 * Copyright 2002-2012 the original author or authors.
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

import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Base class for correlating publisher confirms to sent messages.
 * Use the {@link RabbitTemplate} methods that include one of
 * these as a parameter; when the publisher confirm is received,
 * the CorrelationData is returned with the ack/nack.
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public class CorrelationData {

	private String id;

	public CorrelationData(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	@Override
	public String toString() {
		return "CorrelationData [id=" + id + "]";
	}
}
