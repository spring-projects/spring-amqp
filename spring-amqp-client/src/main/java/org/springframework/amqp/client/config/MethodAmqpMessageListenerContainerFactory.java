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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.client.AmqpConnectionFactory;
import org.springframework.amqp.client.listener.AmqpListenerErrorHandler;
import org.springframework.amqp.listener.adapter.AmqpMessageHandlerMethodFactory;
import org.springframework.amqp.listener.adapter.ReplyPostProcessor;
import org.springframework.amqp.support.AmqpHeaderMapper;
import org.springframework.amqp.support.converter.BytesToStringConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.validation.Validator;

/**
 * The {@link AmqpMessageListenerContainerFactory} extension for {@link MethodAmqpListenerEndpoint}.
 * <p>
 * The {@link MethodAmqpListenerEndpoint}-related properties of this factory serve as defaults values
 * if they are not provided on the {@link MethodAmqpListenerEndpoint}.
 * <p>
 * Delegates to super class if provided endpoint is not a {@link MethodAmqpListenerEndpoint}.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see MethodAmqpListenerEndpoint
 */
public class MethodAmqpMessageListenerContainerFactory extends AmqpMessageListenerContainerFactory
		implements BeanFactoryAware {

	@SuppressWarnings("NullAway.Init")
	private BeanFactory beanFactory;

	private boolean defaultRequeueRejected;

	private boolean returnExceptions;

	private @Nullable ReplyPostProcessor replyPostProcessor;

	private @Nullable AmqpListenerErrorHandler listenerErrorHandler;

	private @Nullable String replyContentType;

	private @Nullable AmqpHeaderMapper headerMapper;

	private @Nullable MessageConverter messageConverter;

	private Charset charset = StandardCharsets.UTF_8;

	private @Nullable Validator validator;

	private List<HandlerMethodArgumentResolver> customMethodArgumentResolvers = new ArrayList<>();

	private @Nullable MessageHandlerMethodFactory messageHandlerMethodFactory;

	/**
	 * Create an instance with the provided {@link AmqpConnectionFactory}.
	 * @param connectionFactory the connection factory to use.
	 */
	public MethodAmqpMessageListenerContainerFactory(AmqpConnectionFactory connectionFactory) {
		super(connectionFactory);
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	/**
	 * Set the default behavior for messages rejection, for example, when the listener
	 * threw an exception. When {@code true} (default), messages will be requeued, otherwise - rejected.
	 * Setting to {@code false} causes all rejections to not be requeued.
	 * When set to {@code true}, the default can be overridden by the listener throwing an
	 * {@link org.springframework.amqp.AmqpRejectAndDontRequeueException}.
	 * @param defaultRequeueRejected false to reject messages by default.
	 * @see MethodAmqpListenerEndpoint#setDefaultRequeueRejected(boolean)
	 */
	public void setDefaultRequeueRejected(boolean defaultRequeueRejected) {
		this.defaultRequeueRejected = defaultRequeueRejected;
	}

	/**
	 * Set whether exceptions thrown by the listener should be returned as a response message body
	 * to the sender using the normal {@code replyTo/@SendTo} semantics.
	 * @param returnExceptions true to return exceptions.
	 * @see MethodAmqpListenerEndpoint#setReturnExceptions(boolean)
	 */
	public void setReturnExceptions(boolean returnExceptions) {
		this.returnExceptions = returnExceptions;
	}

	/**
	 * Set a {@link ReplyPostProcessor} to post process a response message before it is sent.
	 * @param replyPostProcessor the post-processor.
	 * @see MethodAmqpListenerEndpoint#setReplyPostProcessor(ReplyPostProcessor)
	 */
	public void setReplyPostProcessor(ReplyPostProcessor replyPostProcessor) {
		this.replyPostProcessor = replyPostProcessor;
	}

	/**
	 * Set the {@link AmqpListenerErrorHandler} to invoke if the listener method throws an exception.
	 * @param errorHandler the error handler.
	 * @see MethodAmqpListenerEndpoint#setErrorHandler(AmqpListenerErrorHandler)
	 */
	public void setListenerErrorHandler(AmqpListenerErrorHandler errorHandler) {
		this.listenerErrorHandler = errorHandler;
	}

	/**
	 * Set the reply content type.
	 * Overrides the one populated by a message converter.
	 * @param replyContentType the content type.
	 * @see MethodAmqpListenerEndpoint#setReplyContentType(String)
	 */
	public void setReplyContentType(String replyContentType) {
		this.replyContentType = replyContentType;
	}

	/**
	 * Set the {@link AmqpHeaderMapper} implementation to use to map the standard AMQP headers.
	 * @param headerMapper the {@link AmqpHeaderMapper} instance.
	 * @see MethodAmqpListenerEndpoint#setHeaderMapper(AmqpHeaderMapper)
	 */
	public void setHeaderMapper(AmqpHeaderMapper headerMapper) {
		this.headerMapper = headerMapper;
	}

	/**
	 * Set the converter to convert incoming messages to listener method arguments,
	 * and objects returned from listener methods for response messages.
	 * @param messageConverter The message converter.
	 * @see MethodAmqpListenerEndpoint#setMessageConverter(MessageConverter)
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Set a charset for {@code byte[]} to String method argument conversion.
	 * @param charset the charset (default UTF-8).
	 * @see #setMessageHandlerMethodFactory(MessageHandlerMethodFactory)
	 */
	public void setCharset(Charset charset) {
		this.charset = charset;
	}

	/**
	 * Set the validator to use if the default message handler factory is used.
	 * @param validator the validator.
	 * @see #setMessageHandlerMethodFactory(MessageHandlerMethodFactory)
	 */
	public void setValidator(Validator validator) {
		this.validator = validator;
	}

	/**
	 * Add custom methods arguments resolvers the default message handler factory.
	 * Default empty list.
	 * @param methodArgumentResolvers the methodArgumentResolvers to assign.
	 * @see #setMessageHandlerMethodFactory(MessageHandlerMethodFactory)
	 */
	public void setCustomMethodArgumentResolvers(HandlerMethodArgumentResolver... methodArgumentResolvers) {
		this.customMethodArgumentResolvers = Arrays.asList(methodArgumentResolvers);
	}

	/**
	 * Set the {@link MessageHandlerMethodFactory} to configure {@link MethodAmqpListenerEndpoint}.
	 * <p>By default, an {@link AmqpMessageHandlerMethodFactory} is used, and it
	 * can be configured further to support additional method arguments
	 * or to customize conversion and validation support.
	 * See {@link DefaultMessageHandlerMethodFactory} Javadoc for more details.
	 * @param messageHandlerMethodFactory the {@link MessageHandlerMethodFactory} instance.
	 * @see AmqpMessageHandlerMethodFactory
	 */
	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}

	@Override
	protected void configureEndpoint(AmqpListenerEndpoint endpoint) {
		if (endpoint instanceof MethodAmqpListenerEndpoint methodEndpoint) {
			methodEndpoint.setBeanFactory(this.beanFactory);
			methodEndpoint.setMessageHandlerMethodFactory(obtainMessageHandlerMethodFactory());

			JavaUtils.INSTANCE
					.acceptIfCondition(!methodEndpoint.isDefaultRequeueRejected(), this.defaultRequeueRejected,
							methodEndpoint::setDefaultRequeueRejected)
					.acceptIfCondition(!methodEndpoint.isReturnExceptions(), this.returnExceptions,
							methodEndpoint::setReturnExceptions)
					.acceptOrElseIfNotNull(methodEndpoint.getReplyPostProcessor(), this.replyPostProcessor,
							methodEndpoint::setReplyPostProcessor)
					.acceptOrElseIfNotNull(methodEndpoint.getErrorHandler(), this.listenerErrorHandler,
							methodEndpoint::setErrorHandler)
					.acceptOrElseIfNotNull(methodEndpoint.getReplyContentType(), this.replyContentType,
							methodEndpoint::setReplyContentType)
					.acceptOrElseIfNotNull(methodEndpoint.getHeaderMapper(), this.headerMapper,
							methodEndpoint::setHeaderMapper)
					.acceptOrElseIfNotNull(methodEndpoint.getMessageConverter(), this.messageConverter,
							methodEndpoint::setMessageConverter);

		}
	}

	private MessageHandlerMethodFactory obtainMessageHandlerMethodFactory() {
		MessageHandlerMethodFactory messageHandlerMethodFactoryToUse = this.messageHandlerMethodFactory;
		if (messageHandlerMethodFactoryToUse == null) {
			messageHandlerMethodFactoryToUse = createDefaultMessageHandlerMethodFactory();
		}
		return messageHandlerMethodFactoryToUse;
	}

	private MessageHandlerMethodFactory createDefaultMessageHandlerMethodFactory() {
		DefaultMessageHandlerMethodFactory defaultFactory = new AmqpMessageHandlerMethodFactory();
		if (this.validator != null) {
			defaultFactory.setValidator(this.validator);
		}
		defaultFactory.setBeanFactory(this.beanFactory);
		var defaultFormattingConversionService = new DefaultFormattingConversionService();
		defaultFormattingConversionService.addConverter(new BytesToStringConverter(this.charset));
		defaultFactory.setConversionService(defaultFormattingConversionService);
		defaultFactory.setCustomArgumentResolvers(this.customMethodArgumentResolvers);
		defaultFactory.setMessageConverter(new GenericMessageConverter(defaultFormattingConversionService));

		defaultFactory.afterPropertiesSet();
		return defaultFactory;
	}

}
