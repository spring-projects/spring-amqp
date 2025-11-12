/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.listener.adapter.DelegatingInvocableHandler;
import org.springframework.amqp.rabbit.listener.adapter.HandlerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.validation.Validator;

/**
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Artem Bilan
 *
 * @since 1.5
 *
 */
public class MultiMethodRabbitListenerEndpoint extends MethodRabbitListenerEndpoint {

	private final List<Method> methods;

	private final @Nullable Method defaultMethod;

	private @Nullable Validator validator;

	/**
	 * Construct an instance for the provided methods, default method and bean.
	 * @param methods the methods.
	 * @param defaultMethod the default method.
	 * @param bean the bean.
	 * @since 2.0.3
	 */
	@SuppressWarnings("this-escape")
	public MultiMethodRabbitListenerEndpoint(List<Method> methods, @Nullable Method defaultMethod, Object bean) {
		this.methods = methods;
		this.defaultMethod = defaultMethod;
		setBean(bean);
	}

	/**
	 * Set a payload validator.
	 * @param validator the validator.
	 * @since 2.3.7
	 */
	public void setValidator(Validator validator) {
		this.validator = validator;
	}

	@Override
	protected HandlerAdapter configureListenerAdapter(MessagingMessageListenerAdapter messageListener) {
		List<InvocableHandlerMethod> invocableHandlerMethods = new ArrayList<>(this.methods.size());
		InvocableHandlerMethod defaultHandler = null;
		MessageHandlerMethodFactory messageHandlerMethodFactory = getMessageHandlerMethodFactory();
		Assert.state(messageHandlerMethodFactory != null,
				"Could not create message listener - MessageHandlerMethodFactory not set");
		Object beanToUse = getBean();
		for (Method method : this.methods) {
			InvocableHandlerMethod handler = messageHandlerMethodFactory
					.createInvocableHandlerMethod(beanToUse, method);
			invocableHandlerMethods.add(handler);
			if (method.equals(this.defaultMethod)) {
				defaultHandler = handler;
			}
		}
		DelegatingInvocableHandler delegatingHandler = new DelegatingInvocableHandler(invocableHandlerMethods,
				defaultHandler, getBean(), getResolver(), getBeanExpressionContext(), this.validator);
		return new HandlerAdapter(delegatingHandler);
	}

}
