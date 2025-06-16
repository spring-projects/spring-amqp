/*
 * Copyright 2014-present the original author or authors.
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

package org.springframework.amqp.rabbit.config;


import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.listener.AbstractRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;

/**
 * A {@link org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint} simply
 * providing the {@link MessageListener} to invoke to process an incoming message for this
 * endpoint.
 *
 * @author Stephane Nicoll
 * @since 1.4
 */
public class SimpleRabbitListenerEndpoint extends AbstractRabbitListenerEndpoint {

	private MessageListener messageListener;


	/**
	 * Set the {@link MessageListener} to invoke when a message matching
	 * the endpoint is received.
	 * @param messageListener the {@link MessageListener} instance.
	 */
	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
	}

	/**
	 * @return the {@link MessageListener} to invoke when a message matching
	 * the endpoint is received.
	 */
	public MessageListener getMessageListener() {
		return this.messageListener;
	}


	@Override
	protected MessageListener createMessageListener(MessageListenerContainer container) {
		return getMessageListener();
	}

	@Override
	protected StringBuilder getEndpointDescription() {
		return super.getEndpointDescription()
				.append(" | messageListener='").append(this.messageListener).append("'");
	}

}
