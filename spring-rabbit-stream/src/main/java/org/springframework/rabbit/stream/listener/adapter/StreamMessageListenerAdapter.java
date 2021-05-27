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

package org.springframework.rabbit.stream.listener.adapter;

import java.lang.reflect.Method;

import org.springframework.amqp.rabbit.listener.adapter.InvocationResult;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.rabbit.stream.listener.StreamMessageListener;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler.Context;

/**
 * A listener adapter that receives native stream messages.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public class StreamMessageListenerAdapter extends MessagingMessageListenerAdapter implements StreamMessageListener {

	/**
	 * Construct an instance with the provided arguments.
	 * @param bean the bean.
	 * @param method the method.
	 * @param returnExceptions true to return exceptions.
	 * @param errorHandler the error handler.
	 */
	public StreamMessageListenerAdapter(Object bean, Method method, boolean returnExceptions,
			RabbitListenerErrorHandler errorHandler) {

		super(bean, method, returnExceptions, errorHandler);
	}

	@Override
	public void onStreamMessage(Message message, Context context) {
		try {
			InvocationResult result = getHandlerAdapter().invoke(null, message, context);
			if (result.getReturnValue() != null) {
				logger.warn("Replies are not currently supported with native Stream listeners");
			}
			else {
				logger.trace("No result object given - no result to handle");
			}
		}
		catch (Exception ex) {
			this.logger.error("Failed to invoke listener", ex);
		}
	}

}
