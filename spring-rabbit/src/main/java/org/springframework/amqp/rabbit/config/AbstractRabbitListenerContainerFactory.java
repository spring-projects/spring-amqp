/*
 * Copyright 2014-2025 the original author or authors.
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

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.MessageAckListener;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.support.micrometer.RabbitListenerObservationConvention;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory}
 * for Spring's base container implementation.
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
		extends BaseRabbitListenerContainerFactory<C>
		implements ApplicationContextAware, ApplicationEventPublisherAware {

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	protected final AtomicInteger counter = new AtomicInteger(); // NOSONAR

	private @Nullable ConnectionFactory connectionFactory;

	private @Nullable ErrorHandler errorHandler;

	private @Nullable MessageConverter messageConverter;

	private @Nullable AcknowledgeMode acknowledgeMode;

	private @Nullable Boolean channelTransacted;

	private @Nullable Executor taskExecutor;

	private @Nullable PlatformTransactionManager transactionManager;

	private @Nullable Integer prefetchCount;

	private @Nullable Boolean globalQos;

	private @Nullable BackOff recoveryBackOff;

	private @Nullable Boolean missingQueuesFatal;

	private @Nullable Boolean mismatchedQueuesFatal;

	private @Nullable ConsumerTagStrategy consumerTagStrategy;

	private @Nullable Long idleEventInterval;

	private @Nullable Long failedDeclarationRetryInterval;

	private @Nullable ApplicationEventPublisher applicationEventPublisher;

	private @Nullable Boolean autoStartup;

	private @Nullable Integer phase;

	private MessagePostProcessor @Nullable [] afterReceivePostProcessors;

	private @Nullable ContainerCustomizer<C> containerCustomizer;

	private boolean batchListener;

	private @Nullable BatchingStrategy batchingStrategy;

	private @Nullable Boolean deBatchingEnabled;

	private @Nullable MessageAckListener messageAckListener;

	private @Nullable RabbitListenerObservationConvention observationConvention;

	private @Nullable Boolean forceStop;

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
	 * Determine whether the container should de-batch batched
	 * messages (true) or call the listener with the batch (false). Default: true.
	 * @param deBatchingEnabled whether to disable de-batching of messages.
	 * @since 2.2
	 * @see AbstractMessageListenerContainer#setDeBatchingEnabled(boolean)
	 */
	public void setDeBatchingEnabled(final Boolean deBatchingEnabled) {
		this.deBatchingEnabled = deBatchingEnabled;
	}

	/**
	 * Apply prefetch to the entire channel.
	 * @param globalQos true for a channel-wide prefetch.
	 * @since 2.2.17
	 * @see com.rabbitmq.client.Channel#basicQos(int, boolean)
	 */
	public void setGlobalQos(boolean globalQos) {
		this.globalQos = globalQos;
	}

	/**
	 * Set a {@link MessageAckListener} to use when ack a message(messages) in
	 * {@link AcknowledgeMode#AUTO} mode.
	 * @param messageAckListener the messageAckListener.
	 * @since 2.4.6
	 */
	public void setMessageAckListener(MessageAckListener messageAckListener) {
		this.messageAckListener = messageAckListener;
	}

	/**
	 * Set an observation convention; used to add additional key/values to observations.
	 * @param observationConvention the convention.
	 * @since 3.0
	 */
	public void setObservationConvention(RabbitListenerObservationConvention observationConvention) {
		this.observationConvention = observationConvention;
	}

	/**
	 * Set to true to stop the container after the current message(s) are processed and
	 * requeue any prefetched. Useful when using exclusive or single-active consumers.
	 * @param forceStop true to stop when current message(s) are processed.
	 * @since 2.4.15
	 */
	public void setForceStop(boolean forceStop) {
		this.forceStop = forceStop;
	}

	@Override
	public C createListenerContainer(@Nullable RabbitListenerEndpoint endpoint) {
		C instance = createContainerInstance();

		JavaUtils javaUtils =
				JavaUtils.INSTANCE
						.acceptIfNotNull(this.connectionFactory, instance::setConnectionFactory)
						.acceptIfNotNull(this.errorHandler, instance::setErrorHandler);
		if (this.messageConverter != null && endpoint != null && endpoint.getMessageConverter() == null) {
			endpoint.setMessageConverter(this.messageConverter);
		}
		Advice[] adviceChain = getAdviceChain();
		javaUtils
				.acceptIfNotNull(this.acknowledgeMode, instance::setAcknowledgeMode)
				.acceptIfNotNull(this.channelTransacted, instance::setChannelTransacted)
				.acceptIfNotNull(getApplicationContext(), instance::setApplicationContext)
				.acceptIfNotNull(this.taskExecutor, instance::setTaskExecutor)
				.acceptIfNotNull(this.transactionManager, instance::setTransactionManager)
				.acceptIfNotNull(this.prefetchCount, instance::setPrefetchCount)
				.acceptIfNotNull(this.globalQos, instance::setGlobalQos)
				.acceptIfNotNull(getDefaultRequeueRejected(), instance::setDefaultRequeueRejected)
				.acceptIfNotNull(adviceChain, instance::setAdviceChain)
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
				.acceptIfNotNull(this.deBatchingEnabled, instance::setDeBatchingEnabled)
				.acceptIfNotNull(this.messageAckListener, instance::setMessageAckListener)
				.acceptIfNotNull(this.batchingStrategy, instance::setBatchingStrategy)
				.acceptIfNotNull(getMicrometerEnabled(), instance::setMicrometerEnabled)
				.acceptIfNotNull(getObservationEnabled(), instance::setObservationEnabled)
				.acceptIfNotNull(this.observationConvention, instance::setObservationConvention)
				.acceptIfNotNull(this.forceStop, instance::setForceStop);
		if (this.batchListener && this.deBatchingEnabled == null) {
			// turn off container debatching by default for batch listeners
			instance.setDeBatchingEnabled(false);
		}
		if (endpoint != null) { // endpoint settings overriding default factory settings
			javaUtils
					.acceptIfNotNull(endpoint.getTaskExecutor(), instance::setTaskExecutor)
					.acceptIfNotNull(endpoint.getAckMode(), instance::setAcknowledgeMode)
					.acceptIfNotNull(endpoint.getBatchingStrategy(), instance::setBatchingStrategy)
					.acceptIfNotNull(endpoint.getId(), instance::setListenerId);
			if (endpoint.getBatchListener() == null) {
				endpoint.setBatchListener(this.batchListener);
			}
		}
		applyCommonOverrides(endpoint, instance);

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
	protected void initializeContainer(C instance, @Nullable RabbitListenerEndpoint endpoint) {
	}

}
