/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import org.springframework.amqp.core.Message;

import com.rabbitmq.client.Channel;

/**
 * A message listener that is aware of the Channel on which the message was received.
 *  
 * @author Mark Pollack
 */
public interface ChannelAwareMessageListener {

	/**
	 * Callback for processing a received Rabbit message.
	 * <p>Implementors are supposed to process the given Message,
	 * typically sending reply messages through the given Session.
	 * @param message the received AMQP message (never <code>null</code>)
	 * @param channel the underlying Rabbit Channel (never <code>null</code>)
	 * @throws Exception 
	 */
	void onMessage(Message message, Channel channel) throws Exception;

}
