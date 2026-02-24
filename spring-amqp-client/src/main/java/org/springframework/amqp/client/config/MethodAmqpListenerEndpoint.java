/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client.config;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.client.listener.AmqpListenerErrorHandler;
import org.springframework.amqp.client.listener.AmqpMessagingListenerAdapter;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.listener.adapter.HandlerAdapter;
import org.springframework.amqp.listener.adapter.ReplyPostProcessor;
import org.springframework.amqp.support.AmqpHeaderMapper;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.expression.BeanResolver;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;

/**
 * The {@link AbstractAmqpListenerEndpoint} implementation for POJO method invocation listener.
 * Usually, instances of this class are created and handled by the framework.
 * <p>
 * The {@link #setMessageHandlerMethodFactory(MessageHandlerMethodFactory)} and {@link #setBeanFactory(BeanFactory)}
 * must be provided before calling {@link #getMessageListener()}.
 * If other options have to be propagated to the {@link AmqpMessagingListenerAdapter},
 * they have to be provided prior to {@link #getMessageListener()} call.
 * <p>
 * If this class is configured by the framework, any not provided options will be populated
 * with default values from the {@link MethodAmqpMessageListenerContainerFactory} (if any).
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class MethodAmqpListenerEndpoint extends AbstractAmqpListenerEndpoint {

	private final Object bean;

	private final Method method;

	private @Nullable MessageHandlerMethodFactory messageHandlerMethodFactory;

	private boolean defaultRequeueRejected;

	private boolean returnExceptions;

	private @Nullable ReplyPostProcessor replyPostProcessor;

	private @Nullable AmqpListenerErrorHandler errorHandler;

	private @Nullable String replyContentType;

	private @Nullable AmqpHeaderMapper headerMapper;

	private @Nullable MessageConverter messageConverter;

	private @Nullable BeanExpressionResolver resolver;

	private @Nullable BeanExpressionContext expressionContext;

	private @Nullable BeanResolver beanResolver;

	/**
	 * Create an instance for the method to invoke on the provided bean.
	 * The {@code addresses} are propagated to the
	 * {@link org.springframework.amqp.client.listener.AmqpMessageListenerContainer}.
	 * @param bean the bean to invoke the method on.
	 * @param method the method to invoke.
	 * @param addresses the addresses to listen to.
	 */
	public MethodAmqpListenerEndpoint(Object bean, Method method, String... addresses) {
		super(addresses);
		this.bean = bean;
		this.method = method;
	}

	/**
	 * Set the {@link MessageHandlerMethodFactory} to build an {@link InvocableHandlerMethod} for the listener method.
	 * Must be provided before calling {@link #getMessageListener()}.
	 * Usually provided by the framework.
	 * @param messageHandlerMethodFactory the factory to use.
	 */
	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}

	/**
	 * Set the default behavior for messages rejection, for example, when the listener
	 * throws an exception. When {@code true} (default), messages will be requeued, otherwise - rejected.
	 * Setting to {@code false} causes all rejections to not be requeued.
	 * When set to {@code true}, the default can be overridden by the listener throwing an
	 * {@link org.springframework.amqp.AmqpRejectAndDontRequeueException}.
	 * @param defaultRequeueRejected {@code false} to reject messages by default.
	 * @see AmqpMessagingListenerAdapter#setDefaultRequeueRejected(boolean)
	 */
	public void setDefaultRequeueRejected(boolean defaultRequeueRejected) {
		this.defaultRequeueRejected = defaultRequeueRejected;
	}

	protected boolean isDefaultRequeueRejected() {
		return this.defaultRequeueRejected;
	}

	/**
	 * Set whether exceptions thrown by the listener should be returned as a response message body
	 * to the sender using the normal {@code replyTo/@SendTo} semantics.
	 * @param returnExceptions true to return exceptions.
	 * @see AmqpMessagingListenerAdapter#setReturnExceptions(boolean)
	 */
	public void setReturnExceptions(boolean returnExceptions) {
		this.returnExceptions = returnExceptions;
	}

	protected boolean isReturnExceptions() {
		return this.returnExceptions;
	}

	/**
	 * Set a {@link ReplyPostProcessor} to post process a response message before it is sent.
	 * @param replyPostProcessor the post-processor.
	 * @see AmqpMessagingListenerAdapter#setReplyPostProcessor(ReplyPostProcessor)
	 */
	public void setReplyPostProcessor(ReplyPostProcessor replyPostProcessor) {
		this.replyPostProcessor = replyPostProcessor;
	}

	protected @Nullable ReplyPostProcessor getReplyPostProcessor() {
		return this.replyPostProcessor;
	}

	/**
	 * Set the {@link AmqpListenerErrorHandler} to invoke if the listener method throws an exception.
	 * @param errorHandler the error handler.
	 * @see AmqpMessagingListenerAdapter#setErrorHandler(AmqpListenerErrorHandler)
	 */
	public void setErrorHandler(AmqpListenerErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	protected @Nullable AmqpListenerErrorHandler getErrorHandler() {
		return this.errorHandler;
	}

	/**
	 * Set the reply content type.
	 * Overrides the one populated by a message converter.
	 * @param replyContentType the content type.
	 * @see AmqpMessagingListenerAdapter#setReplyContentType(String)
	 */
	public void setReplyContentType(String replyContentType) {
		this.replyContentType = replyContentType;
	}

	protected @Nullable String getReplyContentType() {
		return this.replyContentType;
	}

	/**
	 * Set the {@link AmqpHeaderMapper} implementation to use to map the standard AMQP headers.
	 * @param headerMapper the {@link AmqpHeaderMapper} instance.
	 * @see AmqpMessagingListenerAdapter#setHeaderMapper(AmqpHeaderMapper)
	 */
	public void setHeaderMapper(AmqpHeaderMapper headerMapper) {
		this.headerMapper = headerMapper;
	}

	protected @Nullable AmqpHeaderMapper getHeaderMapper() {
		return this.headerMapper;
	}

	/**
	 * Set the converter to convert incoming messages to listener method arguments,
	 * and objects returned from listener methods for response messages.
	 * @param messageConverter The message converter.
	 * @see AmqpMessagingListenerAdapter#setMessageConverter(MessageConverter)
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	protected @Nullable MessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Provide a {@link BeanFactory} to use for resolving expressions in the listener method.
	 * Usually populated by the framework.
	 * @param beanFactory the {@link BeanFactory} to use.
	 */
	public void setBeanFactory(BeanFactory beanFactory) {
		if (beanFactory instanceof ConfigurableListableBeanFactory clbf) {
			this.resolver = clbf.getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext(clbf, null);
		}

		this.beanResolver = new BeanFactoryResolver(beanFactory);
	}

	@Override
	public MessageListener getMessageListener() {
		Assert.state(this.messageHandlerMethodFactory != null, "A 'MessageHandlerMethodFactory' must be provided.");
		Assert.state(this.beanResolver != null, "A 'BeanFactory' must be provided.");

		String id = getId();
		if (id == null) {
			setId(this.bean.getClass().getName() + "." + this.method.getName() + ".listener");
		}

		InvocableHandlerMethod invocableHandlerMethod =
				this.messageHandlerMethodFactory.createInvocableHandlerMethod(this.bean, this.method);
		HandlerAdapter handlerAdapter = new HandlerAdapter(invocableHandlerMethod);
		AmqpMessagingListenerAdapter amqpMessagingListenerAdapter = new AmqpMessagingListenerAdapter(handlerAdapter);
		amqpMessagingListenerAdapter.setBeanResolver(this.beanResolver);
		amqpMessagingListenerAdapter.setDefaultRequeueRejected(this.defaultRequeueRejected);
		amqpMessagingListenerAdapter.setReturnExceptions(this.returnExceptions);

		JavaUtils.INSTANCE
				.acceptIfNotNull(this.replyContentType, amqpMessagingListenerAdapter::setReplyContentType)
				.acceptIfNotNull(this.replyPostProcessor, amqpMessagingListenerAdapter::setReplyPostProcessor)
				.acceptIfNotNull(this.errorHandler, amqpMessagingListenerAdapter::setErrorHandler)
				.acceptIfNotNull(this.headerMapper, amqpMessagingListenerAdapter::setHeaderMapper)
				.acceptIfNotNull(this.messageConverter, amqpMessagingListenerAdapter::setMessageConverter)
				.acceptIfHasText(resolveDefaultReplyTo(), amqpMessagingListenerAdapter::setResponseAddress);

		return amqpMessagingListenerAdapter;
	}

	private @Nullable String resolveDefaultReplyTo() {
		SendTo ann = AnnotationUtils.getAnnotation(this.method, SendTo.class);
		if (ann != null) {
			String[] destinations = ann.value();
			if (destinations.length > 1) {
				throw new IllegalStateException("Invalid @" + SendTo.class.getSimpleName() + " annotation on '"
						+ this.method + "' one destination must be set (got " + Arrays.toString(destinations) + ")");
			}
			return destinations.length == 1 ? resolveSendTo(destinations[0]) : null;
		}
		return null;
	}

	private String resolveSendTo(String value) {
		if (this.expressionContext != null) {
			String resolvedValue = this.expressionContext.getBeanFactory().resolveEmbeddedValue(value);
			if (this.resolver != null) {
				Object newValue = this.resolver.evaluate(resolvedValue, this.expressionContext);
				Assert.isInstanceOf(String.class, newValue, "Invalid @SendTo expression");
				return (String) newValue;
			}
		}

		return value;
	}

}
