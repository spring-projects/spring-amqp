/*
 * Copyright 2015-2016 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.adapter.DelegatingInvocableHandler;
import org.springframework.amqp.rabbit.listener.adapter.HandlerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * @author Gary Russell
 * @since 1.5
 *
 */
public class MultiMethodRabbitListenerEndpoint extends MethodRabbitListenerEndpoint {

	private final List<Method> methods;

	private DelegatingInvocableHandler delegatingHandler;

	public MultiMethodRabbitListenerEndpoint(List<Method> methods, Object bean) {
		this.methods = methods;
		setBean(bean);
	}

	@Override
	protected HandlerAdapter configureListenerAdapter(MessagingMessageListenerAdapter messageListener) {
		List<InvocableHandlerMethod> invocableHandlerMethods = new ArrayList<InvocableHandlerMethod>();
		for (Method method : this.methods) {
			invocableHandlerMethods.add(getMessageHandlerMethodFactory()
					.createInvocableHandlerMethod(getBean(), method));
		}
		this.delegatingHandler = new DelegatingInvocableHandler(invocableHandlerMethods, getBean(), getResolver(),
				getBeanExpressionContext());
		return new HandlerAdapter(this.delegatingHandler);
	}


	@Override
	protected MessagingMessageListenerAdapter createMessageListenerInstance() {
		return new MultiMethodMessageListenerAdapter();
	}


	private final class MultiMethodMessageListenerAdapter extends MessagingMessageListenerAdapter {

		@Override
		protected Address getReplyToAddress(Message request) throws Exception {
			Address replyTo = request.getMessageProperties().getReplyToAddress();
			Address defaultReplyTo = null;
			if (MultiMethodRabbitListenerEndpoint.this.delegatingHandler != null) {
				defaultReplyTo = MultiMethodRabbitListenerEndpoint.this.delegatingHandler.getDefaultReplyTo();
			}
			if (replyTo == null && defaultReplyTo == null) {
				throw new AmqpException(
						"Cannot determine ReplyTo message property value: " +
								"Request message does not contain reply-to property, " +
								"and no @SendTo annotation found.");
			}
			return replyTo == null ? defaultReplyTo : replyTo;
		}

	}

}
