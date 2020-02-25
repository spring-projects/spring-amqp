/*
 * Copyright 2014-2020 the original author or authors.
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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.adapter.AbstractAdaptableMessageListener;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.lang.Nullable;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Base {@link RabbitListenerContainerFactory} for Spring's base container implementation.
 * @param <C> the container type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Joris Kuipers
 *
 * @since 1.4
 *
 * @see AbstractMessageListenerContainer
 */
public abstract class AbstractRabbitListenerContainerFactory<C extends AbstractMessageListenerContainer>
		implements RabbitListenerContainerFactory<C>, ApplicationContextAware, ApplicationEventPublisherAware {

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	protected final AtomicInteger counter = new AtomicInteger(); // NOSONAR

	private ConnectionFactory connectionFactory;

	private ErrorHandler errorHandler;

	private MessageConverter messageConverter;

	private AcknowledgeMode acknowledgeMode;

	private Boolean channelTransacted;

	private Executor taskExecutor;

	private PlatformTransactionManager transactionManager;

	private Integer prefetchCount;

	private Boolean defaultRequeueRejected;

	private Advice[] adviceChain;

	private BackOff recoveryBackOff;

	private Boolean missingQueuesFatal;

	private Boolean mismatchedQueuesFatal;

	private ConsumerTagStrategy consumerTagStrategy;

	private Long idleEventInterval;

	private Long failedDeclarationRetryInterval;

	private ApplicationEventPublisher applicationEventPublisher;

	private ApplicationContext applicationContext;

	private Boolean autoStartup;

	private Integer phase;

	private MessagePostProcessor[] afterReceivePostProcessors;

	private MessagePostProcessor[] beforeSendReplyPostProcessors;

	private RetryTemplate retryTemplate;

	private RecoveryCallback<?> recoveryCallback;

	private ContainerCustomizer<C> containerCustomizer;

	private boolean batchListener;

	private BatchingStrategy batchingStrategy;

	private Boolean deBatchingEnabled;

	/**
	 * @param connectionFactory The connection factory.
	 * @see AbstractMessageListenerContainer#setConnectionFactory(ConnectionFactory)
	 */
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @param errorHandler The error handler.
	 * @see AbstractMessageListenerContainer#setErrorHandler(org.springframework.util.ErrorHandler)
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * @param messageConverter the message converter to use
	 * @see RabbitListenerEndpoint#setMessageConverter(MessageConverter)
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * @param acknowledgeMode the acknowledge mode to set. Defaults to {@link AcknowledgeMode#AUTO}
	 * @see AbstractMessageListenerContainer#setAcknowledgeMode(AcknowledgeMode)
	 */
	public void setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
		this.acknowledgeMode = acknowledgeMode;
	}

	/**
	 * @param channelTransacted the flag value to set
	 * @see AbstractMessageListenerContainer#setChannelTransacted
	 */
	public void setChannelTransacted(Boolean channelTransacted) {
		this.channelTransacted = channelTransacted;
	}

	/**
	 * @param taskExecutor the {@link Executor} to use.
	 * @see AbstractMessageListenerContainer#setTaskExecutor
	 */
	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * @param transactionManager the {@link PlatformTransactionManager} to use.
	 * @see AbstractMessageListenerContainer#setTransactionManager
	 */
	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	/**
	 * @param prefetch the prefetch count
	 * @see AbstractMessageListenerContainer#setPrefetchCount(int)
	 */
	public void setPrefetchCount(Integer prefetch) {
		this.prefetchCount = prefetch;
	}

	/**
	 * @param requeueRejected true to reject by default.
	 * @see AbstractMessageListenerContainer#setDefaultRequeueRejected
	 */
	public void setDefaultRequeueRejected(Boolean requeueRejected) {
		this.defaultRequeueRejected = requeueRejected;
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

	/**
	 * @param recoveryInterval The recovery interval.
	 * @see AbstractMessageListenerContainer#setRecoveryInterval
	 */
	public void setRecoveryInterval(Long recoveryInterval) {
		this.recoveryBackOff = new FixedBackOff(recoveryInterval, FixedBackOff.UNLIMITED_ATTEMPTS);
	}

	/**
	 * @param recoveryBackOff The BackOff to recover.
	 * @since 1.5
	 * @see AbstractMessageListenerContainer#setRecoveryBackOff(BackOff)
	 */
	public void setRecoveryBackOff(BackOff recoveryBackOff) {
		this.recoveryBackOff = recoveryBackOff;
	}

	/**
	 * @param missingQueuesFatal the missingQueuesFatal to set.
	 * @see AbstractMessageListenerContainer#setMissingQueuesFatal
	 */
	public void setMissingQueuesFatal(Boolean missingQueuesFatal) {
		this.missingQueuesFatal = missingQueuesFatal;
	}

	/**
	 * @param mismatchedQueuesFatal the mismatchedQueuesFatal to set.
	 * @since 1.6
	 * @see AbstractMessageListenerContainer#setMismatchedQueuesFatal(boolean)
	 */
	public void setMismatchedQueuesFatal(Boolean mismatchedQueuesFatal) {
		this.mismatchedQueuesFatal = mismatchedQueuesFatal;
	}

	/**
	 * @param consumerTagStrategy the consumerTagStrategy to set
	 * @see AbstractMessageListenerContainer#setConsumerTagStrategy(ConsumerTagStrategy)
	 */
	public void setConsumerTagStrategy(ConsumerTagStrategy consumerTagStrategy) {
		this.consumerTagStrategy = consumerTagStrategy;
	}

	/**
	 * How often to publish idle container events.
	 * @param idleEventInterval the interval.
	 */
	public void setIdleEventInterval(Long idleEventInterval) {
		this.idleEventInterval = idleEventInterval;
	}

	public void setFailedDeclarationRetryInterval(Long failedDeclarationRetryInterval) {
		this.failedDeclarationRetryInterval = failedDeclarationRetryInterval;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	/**
	 * @param autoStartup true for auto startup.
	 * @see AbstractMessageListenerContainer#setAutoStartup(boolean)
	 */
	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * @param phase The phase.
	 * @see AbstractMessageListenerContainer#setPhase(int)
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * Set post processors which will be applied after the Message is received.
	 * @param postProcessors the post processors.
	 * @since 2.0
	 * @see AbstractMessageListenerContainer#setAfterReceivePostProcessors(MessagePostProcessor...)
	 */
	public void setAfterReceivePostProcessors(MessagePostProcessor... postProcessors) {
		Assert.notNull(postProcessors, "'postProcessors' cannot be null");
		Assert.noNullElements(postProcessors, "'postProcessors' cannot have null elements");
		this.afterReceivePostProcessors = Arrays.copyOf(postProcessors, postProcessors.length);
	}

	/**
	 * Set post processors that will be applied before sending replies; added to each
	 * message listener adapter.
	 * @param postProcessors the post processors.
	 * @since 2.0.3
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
	 * @since 2.0.6
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
	 * @since 2.0.6
	 * @see #setRetryTemplate(RetryTemplate)
	 * @see AbstractAdaptableMessageListener#setRecoveryCallback(RecoveryCallback)
	 */
	public void setReplyRecoveryCallback(RecoveryCallback<?> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	/**
	 * A {@link Consumer} that is invoked to enable setting other container properties not
	 * exposed  by this container factory.
	 * @param configurer the configurer;
	 * @since 2.1.1
	 * @deprecated in favor of {@link #setContainerCustomizer(ContainerCustomizer)}.
	 */
	@Deprecated
	public void setContainerConfigurer(Consumer<C> configurer) {
		this.containerCustomizer = container -> configurer.accept(container);
	}

	/**
	 * Set a {@link ContainerCustomizer} that is invoked after a container is created and
	 * configured to enable further customization of the container.
	 * @param containerCustomizer the customizer.
	 * @since 2.2.2
	 */
	public void setContainerCustomizer(ContainerCustomizer<C> containerCustomizer) {
		this.containerCustomizer = containerCustomizer;
	}

	/**
	 * Set to true to receive a list of debatched messages that were created by a
	 * {@link org.springframework.amqp.rabbit.core.BatchingRabbitTemplate}.
	 * @param isBatch true for a batch listener.
	 * @since 2.2
	 * @see #setBatchingStrategy(BatchingStrategy)
	 */
	public void setBatchListener(boolean isBatch) {
		this.batchListener = isBatch;
	}

	/**
	 * Set a {@link BatchingStrategy} to use when debatching messages.
	 * @param batchingStrategy the batching strategy.
	 * @since 2.2
	 * @see #setBatchListener(boolean)
	 */
	public void setBatchingStrategy(BatchingStrategy batchingStrategy) {
		this.batchingStrategy = batchingStrategy;
	}

	/**
	 * Determine whether or not the container should de-batch batched
	 * messages (true) or call the listener with the batch (false). Default: true.
	 * @param deBatchingEnabled whether or not to disable de-batching of messages.
	 * @since 2.2
	 * @see AbstractMessageListenerContainer#setDeBatchingEnabled(boolean)
	 */
	public void setDeBatchingEnabled(final Boolean deBatchingEnabled) {
		this.deBatchingEnabled = deBatchingEnabled;
	}

	@Override
	public C createListenerContainer(RabbitListenerEndpoint endpoint) {
		C instance = createContainerInstance();

		JavaUtils javaUtils =
				JavaUtils.INSTANCE
						.acceptIfNotNull(this.connectionFactory, instance::setConnectionFactory)
						.acceptIfNotNull(this.errorHandler, instance::setErrorHandler);
		if (this.messageConverter != null && endpoint != null) {
			endpoint.setMessageConverter(this.messageConverter);
		}
		javaUtils
			.acceptIfNotNull(this.acknowledgeMode, instance::setAcknowledgeMode)
			.acceptIfNotNull(this.channelTransacted, instance::setChannelTransacted)
			.acceptIfNotNull(this.applicationContext, instance::setApplicationContext)
			.acceptIfNotNull(this.taskExecutor, instance::setTaskExecutor)
			.acceptIfNotNull(this.transactionManager, instance::setTransactionManager)
			.acceptIfNotNull(this.prefetchCount, instance::setPrefetchCount)
			.acceptIfNotNull(this.defaultRequeueRejected, instance::setDefaultRequeueRejected)
			.acceptIfNotNull(this.adviceChain, instance::setAdviceChain)
			.acceptIfNotNull(this.recoveryBackOff, instance::setRecoveryBackOff)
			.acceptIfNotNull(this.mismatchedQueuesFatal, instance::setMismatchedQueuesFatal)
			.acceptIfNotNull(this.missingQueuesFatal, instance::setMissingQueuesFatal)
			.acceptIfNotNull(this.consumerTagStrategy, instance::setConsumerTagStrategy)
			.acceptIfNotNull(this.idleEventInterval, instance::setIdleEventInterval)
			.acceptIfNotNull(this.failedDeclarationRetryInterval, instance::setFailedDeclarationRetryInterval)
			.acceptIfNotNull(this.applicationEventPublisher, instance::setApplicationEventPublisher)
			.acceptIfNotNull(this.autoStartup, instance::setAutoStartup)
			.acceptIfNotNull(this.phase, instance::setPhase)
			.acceptIfNotNull(this.afterReceivePostProcessors, instance::setAfterReceivePostProcessors)
			.acceptIfNotNull(this.deBatchingEnabled, instance::setDeBatchingEnabled);
		if (this.batchListener && this.deBatchingEnabled == null) {
			// turn off container debatching by default for batch listeners
			instance.setDeBatchingEnabled(false);
		}
		if (endpoint != null) { // endpoint settings overriding default factory settings
			javaUtils
				.acceptIfNotNull(endpoint.getAutoStartup(), instance::setAutoStartup)
				.acceptIfNotNull(endpoint.getTaskExecutor(), instance::setTaskExecutor)
				.acceptIfNotNull(endpoint.getAckMode(), instance::setAcknowledgeMode);
			javaUtils
				.acceptIfNotNull(this.batchingStrategy, endpoint::setBatchingStrategy);
			instance.setListenerId(endpoint.getId());
			endpoint.setBatchListener(this.batchListener);
			endpoint.setupListenerContainer(instance);
		}
		if (instance.getMessageListener() instanceof AbstractAdaptableMessageListener) {
			AbstractAdaptableMessageListener messageListener = (AbstractAdaptableMessageListener) instance
					.getMessageListener();
			javaUtils
					.acceptIfNotNull(this.beforeSendReplyPostProcessors,
							messageListener::setBeforeSendReplyPostProcessors)
					.acceptIfNotNull(this.retryTemplate, messageListener::setRetryTemplate)
					.acceptIfCondition(this.retryTemplate != null && this.recoveryCallback != null,
							this.recoveryCallback, messageListener::setRecoveryCallback)
					.acceptIfNotNull(this.defaultRequeueRejected, messageListener::setDefaultRequeueRejected)
					.acceptIfNotNull(endpoint.getReplyPostProcessor(), messageListener::setReplyPostProcessor);
		}
		initializeContainer(instance, endpoint);

		if (this.containerCustomizer != null) {
			this.containerCustomizer.configure(instance);
		}

		return instance;
	}

	/**
	 * Create an empty container instance.
	 * @return the new container instance.
	 */
	protected abstract C createContainerInstance();

	/**
	 * Further initialize the specified container.
	 * <p>Subclasses can inherit from this method to apply extra
	 * configuration if necessary.
	 * @param instance the container instance to configure.
	 * @param endpoint the endpoint.
	 */
	protected void initializeContainer(C instance, RabbitListenerEndpoint endpoint) {
	}

}
