/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.remoting.testhelper;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;

/**
 * @author David Bilge
 * @author Gary Russell
 * @since 1.2
 */
public class SentSavingTemplate extends AbstractAmqpTemplate {

	private Message lastMessage = null;

	private String lastExchange = null;

	private String lastRoutingKey = null;

	@Override
	public void send(String exchange, String routingKey, Message message) throws AmqpException {
		this.lastExchange = exchange;
		this.lastRoutingKey = routingKey;
		this.lastMessage = message;
	}

	public Message getLastMessage() {
		return lastMessage;
	}

	public String getLastExchange() {
		return lastExchange;
	}

	public String getLastRoutingKey() {
		return lastRoutingKey;
	}

}
