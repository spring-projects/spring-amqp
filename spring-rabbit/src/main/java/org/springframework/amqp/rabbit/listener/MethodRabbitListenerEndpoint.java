/*
 * Copyright 2014-2022 the original author or authors.
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
import java.util.Arrays;

import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.listener.adapter.BatchMessagingMessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.HandlerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.lang.Nullable;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;

/**
 * A {@link RabbitListenerEndpoint} providing the method to invoke to process
 * an incoming message for this endpoint.
 *
 * @author Stephane Nicoll
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.4
 */
public class MethodRabbitListenerEndpoint extends AbstractRabbitListenerEndpoint {

	private Object bean;

	private Method method;

	private MessageHandlerMethodFactory messageHandlerMethodFactory;

	private boolean returnExceptions;

	private RabbitListenerErrorHandler errorHandler;

	private AdapterProvider adapterProvider = new DefaultAdapterProvider();

	/**
	 * Set the object instance that should manage this endpoint.
	 * @param bean the target bean instance.
	 */
	public void setBean(Object bean) {
		this.bean = bean;
	}

	public Object getBean() {
		return this.bean;
	}

	/**
	 * Set the method to invoke to process a message managed by this endpoint.
	 * @param method the target method for the {@link #bean}.
	 */
	public void setMethod(Method method) {
		this.method = method;
	}

	public Method getMethod() {
		return this.method;
	}

	/**
	 * Set the {@link MessageHandlerMethodFactory} to use to build the
	 * {@link InvocableHandlerMethod} responsible to manage the invocation
	 * of this endpoint.
	 * @param messageHandlerMethodFactory the {@link MessageHandlerMethodFactory} instance.
	 */
	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}

	/**
	 * Set whether exceptions thrown by the listener should be returned to the sender
	 * using the normal {@code replyTo/@SendTo} semantics.
	 * @param returnExceptions true to return exceptions.
	 * @since 2.0
	 */
	public void setReturnExceptions(boolean returnExceptions) {
		this.returnExceptions = returnExceptions;
	}

	/**
	 * Set the {@link RabbitListenerErrorHandler} to invoke if the listener method
	 * throws an exception.
	 * @param errorHandler the error handler.
	 * @since 2.0
	 */
	public void setErrorHandler(RabbitListenerErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * @return the messageHandlerMethodFactory
	 */
	protected MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
		return this.messageHandlerMethodFactory;
	}

	/**
	 * Set a provider to create adapter instances.
	 * @param adapterProvider the provider.
	 */
	public void setAdapterProvider(AdapterProvider adapterProvider) {
		Assert.notNull(adapterProvider, "'adapterProvider' cannot be null");
		this.adapterProvider = adapterProvider;
	}

	@Override
	protected MessagingMessageListenerAdapter createMessageListener(MessageListenerContainer container) {
		Assert.state(this.messageHandlerMethodFactory != null,
				"Could not create message listener - MessageHandlerMethodFactory not set");
		MessagingMessageListenerAdapter messageListener = createMessageListenerInstance(getBatchListener());
		messageListener.setHandlerAdapter(configureListenerAdapter(messageListener));
		String replyToAddress = getDefaultReplyToAddress();
		if (replyToAddress != null) {
			messageListener.setResponseAddress(replyToAddress);
		}
		MessageConverter messageConverter = getMessageConverter();
		if (messageConverter != null) {
			messageListener.setMessageConverter(messageConverter);
		}
		if (getBeanResolver() != null) {
			messageListener.setBeanResolver(getBeanResolver());
		}
		return messageListener;
	}

	/**
	 * Create a {@link HandlerAdapter} for this listener adapter.
	 * @param messageListener the listener adapter.
	 * @return the handler adapter.
	 */
	protected HandlerAdapter configureListenerAdapter(MessagingMessageListenerAdapter messageListener) {
		InvocableHandlerMethod invocableHandlerMethod =
				this.messageHandlerMethodFactory.createInvocableHandlerMethod(getBean(), getMethod());
		return new HandlerAdapter(invocableHandlerMethod);
	}

	/**
	 * Create an empty {@link MessagingMessageListenerAdapter} instance.
	 * @param batch whether this endpoint is for a batch listener.
	 * @return the {@link MessagingMessageListenerAdapter} instance.
	 */
	protected MessagingMessageListenerAdapter createMessageListenerInstance(@Nullable Boolean batch) {
		return this.adapterProvider.getAdapter(batch == null ? isBatchListener() : batch, this.bean, this.method,
				this.returnExceptions, this.errorHandler, getBatchingStrategy());
	}

	@Nullable
	private String getDefaultReplyToAddress() {
		Method listenerMethod = getMethod();
		if (listenerMethod != null) {
			SendTo ann = AnnotationUtils.getAnnotation(listenerMethod, SendTo.class);
			if (ann != null) {
				String[] destinations = ann.value();
				if (destinations.length > 1) {
					throw new IllegalStateException("Invalid @" + SendTo.class.getSimpleName() + " annotation on '"
							+ listenerMethod + "' one destination must be set (got " + Arrays.toString(destinations) + ")");
				}
				return destinations.length == 1 ? resolveSendTo(destinations[0]) : "";
			}
		}
		return null;
	}

	private String resolveSendTo(String value) {
		if (getBeanFactory() != null) {
			String resolvedValue = getBeanExpressionContext().getBeanFactory().resolveEmbeddedValue(value);
			Object newValue = getResolver().evaluate(resolvedValue, getBeanExpressionContext());
			Assert.isInstanceOf(String.class, newValue, "Invalid @SendTo expression");
			return (String) newValue;
		}
		else {
			return value;
		}
	}

	@Override
	protected StringBuilder getEndpointDescription() {
		return super.getEndpointDescription()
				.append(" | bean='").append(this.bean).append("'")
				.append(" | method='").append(this.method).append("'");
	}

	/**
	 * Provider of listener adapters.
	 * @since 2.4
	 *
	 */
	public interface AdapterProvider {

		/**
		 * Get an adapter instance.
		 * @param batch true for a batch listener.
		 * @param bean the bean.
		 * @param method the method.
		 * @param returnExceptions true to return exceptions.
		 * @param errorHandler the error handler.
		 * @param batchingStrategy the batching strategy for batch listeners.
		 * @return the adapter.
		 */
		MessagingMessageListenerAdapter getAdapter(boolean batch, Object bean, Method method, boolean returnExceptions,
				RabbitListenerErrorHandler errorHandler, @Nullable BatchingStrategy batchingStrategy);
	}

	private static final class DefaultAdapterProvider implements AdapterProvider {

		@Override
		public MessagingMessageListenerAdapter getAdapter(boolean batch, Object bean, Method method,
				boolean returnExceptions, RabbitListenerErrorHandler errorHandler,
				@Nullable BatchingStrategy batchingStrategy) {

			if (batch) {
				return new BatchMessagingMessageListenerAdapter(bean, method, returnExceptions, errorHandler,
						batchingStrategy);
			}
			else {
				return new MessagingMessageListenerAdapter(bean, method, returnExceptions, errorHandler);
			}
		}

	}

}
