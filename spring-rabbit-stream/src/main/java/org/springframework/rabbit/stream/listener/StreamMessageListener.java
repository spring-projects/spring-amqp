/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.rabbit.stream.listener;

import org.springframework.amqp.core.MessageListener;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler.Context;

/**
 * A message listener that receives native stream messages.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public interface StreamMessageListener extends MessageListener {

	/**
	 * Process a message.
	 * @param message the message.
	 * @param context the stream context.
	 */
	void onStreamMessage(Message message, Context context);

}
