/*
 * Copyright 2021-2022 the original author or authors.
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

import java.util.Arrays;
import java.util.function.Function;

import org.aopalliance.aop.Advice;

import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.adapter.AbstractAdaptableMessageListener;
import org.springframework.amqp.rabbit.listener.adapter.ReplyPostProcessor;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.lang.Nullable;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * Base abstract class for listener container factories.
 *
 * @param <C> the container type that the factory creates.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public abstract class BaseRabbitListenerContainerFactory<C extends MessageListenerContainer>
	implements RabbitListenerContainerFactory<C> {

	private Boolean defaultRequeueRejected;

	private MessagePostProcessor[] beforeSendReplyPostProcessors;

	private RetryTemplate retryTemplate;

	private RecoveryCallback<?> recoveryCallback;

	private Advice[] adviceChain;

	private Function<String, ReplyPostProcessor> replyPostProcessorProvider;

	@Override
	public abstract C createListenerContainer(RabbitListenerEndpoint endpoint);

	/**
	 * @param requeueRejected true to reject by default.
	 * @see org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer#setDefaultRequeueRejected
	 */
	public void setDefaultRequeueRejected(Boolean requeueRejected) {
		this.defaultRequeueRejected = requeueRejected;
	}

	/**
	 * Return the defaultRequeueRejected.
	 * @return the defaultRequeueRejected.
	 */
	protected Boolean getDefaultRequeueRejected() {
		return this.defaultRequeueRejected;
	}

	/**
	 * Set post processors that will be applied before sending replies; added to each
	 * message listener adapter.
	 * @param postProcessors the post processors.
	 * @see AbstractAdaptableMessageListener#setBeforeSendReplyPostProcessors(MessagePostProcessor...)
	 */
	public void setBeforeSendReplyPostProcessors(MessagePostProcessor... postProcessors) {
		Assert.notNull(postProcessors, "'postProcessors' cannot be null");
		Assert.noNullElements(postProcessors, "'postProcessors' cannot have null elements");
		this.beforeSendReplyPostProcessors = Arrays.copyOf(postProcessors, postProcessors.length);
	}

	/**
	 * Set a {@link RetryTemplate} to use when sending replies; added to each message
	 * listener adapter.
	 * @param retryTemplate the template.
	 * @see #setReplyRecoveryCallback(RecoveryCallback)
	 * @see AbstractAdaptableMessageListener#setRetryTemplate(RetryTemplate)
	 */
	public void setRetryTemplate(RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
	}

	/**
	 * Set a {@link RecoveryCallback} to invoke when retries are exhausted. Added to each
	 * message listener adapter. Only used if a {@link #setRetryTemplate(RetryTemplate)
	 * retryTemplate} is provided.
	 * @param recoveryCallback the recovery callback.
	 * @see #setRetryTemplate(RetryTemplate)
	 * @see AbstractAdaptableMessageListener#setRecoveryCallback(RecoveryCallback)
	 */
	public void setReplyRecoveryCallback(RecoveryCallback<?> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	/**
	 * Set a function to provide a reply post processor; it will be used if there is no
	 * replyPostProcessor on the rabbit listener annotation. The input parameter is the
	 * listener id.
	 * @param replyPostProcessorProvider the post processor.
	 * @since 3.0
	 */
	public void setReplyPostProcessorProvider(Function<String, ReplyPostProcessor> replyPostProcessorProvider) {
		this.replyPostProcessorProvider = replyPostProcessorProvider;
	}

	protected void applyCommonOverrides(@Nullable RabbitListenerEndpoint endpoint, C instance) {
		if (endpoint != null) { // endpoint settings overriding default factory settings
			JavaUtils.INSTANCE
				.acceptIfNotNull(endpoint.getAutoStartup(), instance::setAutoStartup);
			instance.setListenerId(endpoint.getId());
			endpoint.setupListenerContainer(instance);
		}
		Object iml = instance.getMessageListener();
		if (iml instanceof AbstractAdaptableMessageListener) {
			AbstractAdaptableMessageListener messageListener = (AbstractAdaptableMessageListener) iml;
			JavaUtils.INSTANCE // NOSONAR
					.acceptIfNotNull(this.beforeSendReplyPostProcessors,
							messageListener::setBeforeSendReplyPostProcessors)
					.acceptIfNotNull(this.retryTemplate, messageListener::setRetryTemplate)
					.acceptIfCondition(this.retryTemplate != null && this.recoveryCallback != null,
							this.recoveryCallback, messageListener::setRecoveryCallback)
					.acceptIfNotNull(this.defaultRequeueRejected, messageListener::setDefaultRequeueRejected);
			if (endpoint != null) {
				JavaUtils.INSTANCE
						.acceptIfNotNull(endpoint.getReplyPostProcessor(), messageListener::setReplyPostProcessor)
						.acceptIfNotNull(endpoint.getReplyContentType(), messageListener::setReplyContentType);
				messageListener.setConverterWinsContentType(endpoint.isConverterWinsContentType());
				if (endpoint.getReplyPostProcessor() == null && this.replyPostProcessorProvider != null) {
					JavaUtils.INSTANCE
							.acceptIfNotNull(this.replyPostProcessorProvider.apply(endpoint.getId()),
									messageListener::setReplyPostProcessor);
				}
			}
		}
	}

	/**
	 * @return the advice chain that was set. Defaults to {@code null}.
	 * @since 1.7.4
	 */
	@Nullable
	public Advice[] getAdviceChain() {
		return this.adviceChain == null ? null : Arrays.copyOf(this.adviceChain, this.adviceChain.length);
	}

	/**
	 * @param adviceChain the advice chain to set.
	 * @see AbstractMessageListenerContainer#setAdviceChain
	 */
	public void setAdviceChain(Advice... adviceChain) {
		this.adviceChain = adviceChain == null ? null : Arrays.copyOf(adviceChain, adviceChain.length);
	}

}
