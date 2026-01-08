/*
 * Copyright 2021-present the original author or authors.
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
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageListenerContainer;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.adapter.AbstractAdaptableMessageListener;
import org.springframework.amqp.rabbit.listener.adapter.ReplyPostProcessor;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.util.Assert;

/**
 * Base abstract class for listener container factories.
 *
 * @param <C> the container type that the factory creates.
 *
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Artem Bilan
 * @author Stephane Nicoll
 *
 * @since 2.4
 *
 */
public abstract class BaseRabbitListenerContainerFactory<C extends MessageListenerContainer>
		implements RabbitListenerContainerFactory<C>, ApplicationContextAware {

	private @Nullable Boolean defaultRequeueRejected;

	private MessagePostProcessor @Nullable [] beforeSendReplyPostProcessors;

	private @Nullable RetryTemplate retryTemplate;

	private @Nullable MessageRecoverer recoveryCallback;

	private Advice @Nullable [] adviceChain;

	private @Nullable Function<@Nullable String, @Nullable ReplyPostProcessor> replyPostProcessorProvider;

	private @Nullable Boolean micrometerEnabled;

	private @Nullable Boolean observationEnabled;

	@SuppressWarnings("NullAway.Init")
	private ApplicationContext applicationContext;

	private @Nullable String beanName;

	@Override
	public abstract C createListenerContainer(@Nullable RabbitListenerEndpoint endpoint);

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
	protected @Nullable Boolean getDefaultRequeueRejected() {
		return this.defaultRequeueRejected;
	}

	/**
	 * Set post-processors that will be applied before sending replies; added to each
	 * message listener adapter.
	 * @param postProcessors the post-processors.
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
	 * @see #setReplyRecoveryCallback(MessageRecoverer)
	 * @see AbstractAdaptableMessageListener#setRetryTemplate(RetryTemplate)
	 */
	public void setRetryTemplate(RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
	}

	/**
	 * Set a {@link MessageRecoverer} to invoke when retries are exhausted. Added to
	 * each message listener adapter. Only used if a {@link #setRetryTemplate(RetryTemplate)
	 * retryTemplate} is provided.
	 * @param recoveryCallback the recovery callback.
	 * @see #setRetryTemplate(RetryTemplate)
	 * @see AbstractAdaptableMessageListener#setRecoveryCallback(MessageRecoverer)
	 */
	public void setReplyRecoveryCallback(MessageRecoverer recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	/**
	 * Set a function to provide a reply post-processor; it will be used if there is no
	 * replyPostProcessor on the rabbit listener annotation. The input parameter is the
	 * listener id.
	 * @param replyPostProcessorProvider the post-processor.
	 * @since 3.0
	 */
	public void setReplyPostProcessorProvider(
			Function<@Nullable String, @Nullable ReplyPostProcessor> replyPostProcessorProvider) {

		this.replyPostProcessorProvider = replyPostProcessorProvider;
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	protected void applyCommonOverrides(@Nullable RabbitListenerEndpoint endpoint, C instance) {
		if (endpoint != null) { // endpoint settings overriding default factory settings
			JavaUtils.INSTANCE
					.acceptIfNotNull(endpoint.getAutoStartup(), instance::setAutoStartup)
					.acceptIfNotNull(endpoint.getId(), instance::setListenerId);
			endpoint.setupListenerContainer(instance);
		}
		Object iml = instance.getMessageListener();
		if (iml instanceof AbstractAdaptableMessageListener messageListener) {
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
	public Advice @Nullable [] getAdviceChain() {
		return this.adviceChain == null ? null : Arrays.copyOf(this.adviceChain, this.adviceChain.length);
	}

	/**
	 * @param adviceChain the advice chain to set.
	 * @see AbstractMessageListenerContainer#setAdviceChain
	 */
	public void setAdviceChain(Advice @Nullable ... adviceChain) {
		this.adviceChain = adviceChain == null ? null : Arrays.copyOf(adviceChain, adviceChain.length);
	}

	/**
	 * Set to {@code false} to disable micrometer listener timers. When true, ignored
	 * if {@link #setObservationEnabled(boolean)} is set to true.
	 * @param micrometerEnabled false to disable.
	 * @since 3.0
	 * @see #setObservationEnabled(boolean)
	 */
	public void setMicrometerEnabled(boolean micrometerEnabled) {
		this.micrometerEnabled = micrometerEnabled;
	}

	protected @Nullable Boolean getMicrometerEnabled() {
		return this.micrometerEnabled;
	}

	/**
	 * Enable observation via micrometer; disables basic Micrometer timers enabled
	 * by {@link #setMicrometerEnabled(boolean)}.
	 * @param observationEnabled true to enable.
	 * @since 3.0
	 * @see #setMicrometerEnabled(boolean)
	 */
	public void setObservationEnabled(boolean observationEnabled) {
		this.observationEnabled = observationEnabled;
	}

	protected @Nullable Boolean getObservationEnabled() {
		return this.observationEnabled;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	protected ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	@Override
	public @Nullable String getBeanName() {
		return this.beanName;
	}

}
