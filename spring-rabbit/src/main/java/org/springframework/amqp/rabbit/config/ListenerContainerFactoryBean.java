/*
 * Copyright 2016-present the original author or authors.
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import org.aopalliance.aop.Advice;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.ConditionalExceptionLogger;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;
import org.springframework.util.backoff.BackOff;

/**
 * A Factory bean to create a listener container.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Johno Crawford
 * @author Jeonggi Kim
 * @author Ngoc Nhan
 *
 * @since 2.0
 *
 */
public class ListenerContainerFactoryBean extends AbstractFactoryBean<AbstractMessageListenerContainer>
		implements ApplicationContextAware, BeanNameAware, ApplicationEventPublisherAware, SmartLifecycle {

	private final Map<String, String> micrometerTags = new HashMap<>();

	private @Nullable ApplicationContext applicationContext;

	private @Nullable String beanName;

	private @Nullable ApplicationEventPublisher applicationEventPublisher;

	private Type type = Type.simple;

	private @Nullable AbstractMessageListenerContainer listenerContainer;

	private @Nullable ConnectionFactory connectionFactory;

	private @Nullable Boolean channelTransacted;

	private @Nullable AcknowledgeMode acknowledgeMode;

	private String @Nullable [] queueNames;

	private Queue @Nullable [] queues;

	private @Nullable Boolean exposeListenerChannel;

	private @Nullable MessageListener messageListener;

	private @Nullable ErrorHandler errorHandler;

	private @Nullable Boolean deBatchingEnabled;

	private Advice @Nullable [] adviceChain;

	private MessagePostProcessor @Nullable [] afterReceivePostProcessors;

	private @Nullable Boolean autoStartup;

	private @Nullable Integer phase;

	private @Nullable String listenerId;

	private @Nullable ConsumerTagStrategy consumerTagStrategy;

	private @Nullable Map<String, Object> consumerArgs;

	private @Nullable Boolean noLocal;

	private @Nullable Boolean exclusive;

	private @Nullable Boolean defaultRequeueRejected;

	private @Nullable Integer prefetchCount;

	private @Nullable Boolean globalQos;

	private @Nullable Long shutdownTimeout;

	private @Nullable Long idleEventInterval;

	private @Nullable PlatformTransactionManager transactionManager;

	private @Nullable TransactionAttribute transactionAttribute;

	private @Nullable Executor taskExecutor;

	private @Nullable Long recoveryInterval;

	private @Nullable BackOff recoveryBackOff;

	private @Nullable MessagePropertiesConverter messagePropertiesConverter;

	private @Nullable RabbitAdmin rabbitAdmin;

	private @Nullable Boolean missingQueuesFatal;

	private @Nullable Boolean possibleAuthenticationFailureFatal;

	private @Nullable Boolean mismatchedQueuesFatal;

	private @Nullable Boolean autoDeclare;

	private @Nullable Long failedDeclarationRetryInterval;

	private @Nullable ConditionalExceptionLogger exclusiveConsumerExceptionLogger;

	private @Nullable Integer consumersPerQueue;

	private @Nullable TaskScheduler taskScheduler;

	private @Nullable Long monitorInterval;

	private @Nullable Integer concurrentConsumers;

	private @Nullable Integer maxConcurrentConsumers;

	private @Nullable Long startConsumerMinInterval;

	private @Nullable Long stopConsumerMinInterval;

	private @Nullable Integer consecutiveActiveTrigger;

	private @Nullable Integer consecutiveIdleTrigger;

	private @Nullable Long receiveTimeout;

	private @Nullable Long batchReceiveTimeout;

	private @Nullable Integer batchSize;

	private @Nullable Integer declarationRetries;

	private @Nullable Long retryDeclarationInterval;

	private @Nullable Boolean consumerBatchEnabled;

	private @Nullable Boolean micrometerEnabled;

	private @Nullable ContainerCustomizer<SimpleMessageListenerContainer> smlcCustomizer;

	private @Nullable ContainerCustomizer<DirectMessageListenerContainer> dmlcCustomizer;

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public void setChannelTransacted(boolean transactional) {
		this.channelTransacted = transactional;
	}

	public void setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
		this.acknowledgeMode = acknowledgeMode;
	}

	public void setQueueNames(String... queueName) { // NOSONAR
		this.queueNames = queueName; // NOSONAR
	}

	public void setQueues(Queue... queues) { // NOSONAR
		this.queues = queues; // NOSONAR
	}

	public void setExposeListenerChannel(boolean exposeListenerChannel) {
		this.exposeListenerChannel = exposeListenerChannel;
	}

	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
	}

	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	public void setDeBatchingEnabled(boolean deBatchingEnabled) {
		this.deBatchingEnabled = deBatchingEnabled;
	}

	public void setAdviceChain(Advice... adviceChain) { // NOSONAR
		this.adviceChain = adviceChain; // NOSONAR
	}

	public void setAfterReceivePostProcessors(MessagePostProcessor... afterReceivePostProcessors) { // NOSONAR
		this.afterReceivePostProcessors = afterReceivePostProcessors; // NOSONAR
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	public void setListenerId(String listenerId) {
		this.listenerId = listenerId;
	}

	public void setConsumerTagStrategy(ConsumerTagStrategy consumerTagStrategy) {
		this.consumerTagStrategy = consumerTagStrategy;
	}

	public void setConsumerArguments(Map<String, Object> args) {
		this.consumerArgs = args;
	}

	public void setNoLocal(Boolean noLocal) {
		this.noLocal = noLocal;
	}

	public void setExclusive(boolean exclusive) {
		this.exclusive = exclusive;
	}

	public void setDefaultRequeueRejected(boolean defaultRequeueRejected) {
		this.defaultRequeueRejected = defaultRequeueRejected;
	}

	public void setPrefetchCount(int prefetchCount) {
		this.prefetchCount = prefetchCount;
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

	public void setShutdownTimeout(long shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}

	public void setIdleEventInterval(long idleEventInterval) {
		this.idleEventInterval = idleEventInterval;
	}

	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	public void setTransactionAttribute(TransactionAttribute transactionAttribute) {
		this.transactionAttribute = transactionAttribute;
	}

	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	public void setRecoveryInterval(long recoveryInterval) {
		this.recoveryInterval = recoveryInterval;
	}

	public void setRecoveryBackOff(BackOff recoveryBackOff) {
		this.recoveryBackOff = recoveryBackOff;
	}

	public void setMessagePropertiesConverter(MessagePropertiesConverter messagePropertiesConverter) {
		this.messagePropertiesConverter = messagePropertiesConverter;
	}

	public void setRabbitAdmin(RabbitAdmin rabbitAdmin) {
		this.rabbitAdmin = rabbitAdmin;
	}

	public void setMissingQueuesFatal(boolean missingQueuesFatal) {
		this.missingQueuesFatal = missingQueuesFatal;
	}

	public void setPossibleAuthenticationFailureFatal(Boolean possibleAuthenticationFailureFatal) {
		this.possibleAuthenticationFailureFatal = possibleAuthenticationFailureFatal;
	}

	public void setMismatchedQueuesFatal(boolean mismatchedQueuesFatal) {
		this.mismatchedQueuesFatal = mismatchedQueuesFatal;
	}

	public void setAutoDeclare(boolean autoDeclare) {
		this.autoDeclare = autoDeclare;
	}

	public void setFailedDeclarationRetryInterval(long failedDeclarationRetryInterval) {
		this.failedDeclarationRetryInterval = failedDeclarationRetryInterval;
	}

	public void setExclusiveConsumerExceptionLogger(ConditionalExceptionLogger exclusiveConsumerExceptionLogger) {
		this.exclusiveConsumerExceptionLogger = exclusiveConsumerExceptionLogger;
	}

	public void setConsumersPerQueue(int consumersPerQueue) {
		this.consumersPerQueue = consumersPerQueue;
	}

	public void setTaskScheduler(TaskScheduler taskScheduler) {
		this.taskScheduler = taskScheduler;
	}

	public void setMonitorInterval(long monitorInterval) {
		this.monitorInterval = monitorInterval;
	}

	public void setConcurrentConsumers(int concurrentConsumers) {
		this.concurrentConsumers = concurrentConsumers;
	}

	public void setMaxConcurrentConsumers(int maxConcurrentConsumers) {
		this.maxConcurrentConsumers = maxConcurrentConsumers;
	}

	public void setStartConsumerMinInterval(long startConsumerMinInterval) {
		this.startConsumerMinInterval = startConsumerMinInterval;
	}

	public void setStopConsumerMinInterval(long stopConsumerMinInterval) {
		this.stopConsumerMinInterval = stopConsumerMinInterval;
	}

	public void setConsecutiveActiveTrigger(int consecutiveActiveTrigger) {
		this.consecutiveActiveTrigger = consecutiveActiveTrigger;
	}

	public void setConsecutiveIdleTrigger(int consecutiveIdleTrigger) {
		this.consecutiveIdleTrigger = consecutiveIdleTrigger;
	}

	public void setReceiveTimeout(long receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	/**
	 * The number of milliseconds of timeout for gathering batch messages.
	 * It limits the time to wait to fill batchSize.
	 * Default is 0 (no timeout).
	 * @param batchReceiveTimeout the timeout for gathering batch messages.
	 * @since 3.1.2
	 * @see #setBatchSize(int)
	 */
	public void setBatchReceiveTimeout(long batchReceiveTimeout) {
		this.batchReceiveTimeout = batchReceiveTimeout;
	}

	/**
	 * This property has several functions.
	 * <p>
	 * When the channel is transacted, it determines how many messages to process in a
	 * single transaction. It should be less than or equal to
	 * {@link #setPrefetchCount(int) the prefetch count}.
	 * <p>
	 * It also affects how often acks are sent when using
	 * {@link org.springframework.amqp.core.AcknowledgeMode#AUTO} - one ack per BatchSize.
	 * <p>
	 * Finally, when {@link #setConsumerBatchEnabled(boolean)} is true, it determines how
	 * many records to include in the batch as long as sufficient messages arrive within
	 * {@link #setReceiveTimeout(long)}.
	 * <p>
	 * <b>IMPORTANT</b> The batch size represents the number of physical messages
	 * received. If {@link #setDeBatchingEnabled(boolean)} is true and a message is a
	 * batch created by a producer, the actual number of messages received by the listener
	 * will be larger than this batch size.
	 * <p>
	 *
	 * Default is 1.
	 * @param batchSize the batch size
	 * @since 2.2
	 */
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	/**
	 * Set to true to present a list of messages based on the {@link #setBatchSize(int)},
	 * if the container and listener support it.
	 * @param consumerBatchEnabled true to create message batches in the container.
	 * @since 2.2
	 * @see #setBatchSize(int)
	 */
	public void setConsumerBatchEnabled(boolean consumerBatchEnabled) {
		this.consumerBatchEnabled = consumerBatchEnabled;
	}

	public void setDeclarationRetries(int declarationRetries) {
		this.declarationRetries = declarationRetries;
	}

	public void setRetryDeclarationInterval(long retryDeclarationInterval) {
		this.retryDeclarationInterval = retryDeclarationInterval;
	}

	/**
	 * Set to false to disable micrometer listener timers.
	 * @param enabled false to disable.
	 * @since 2.4.6
	 */
	public void setMicrometerEnabled(boolean enabled) {
		this.micrometerEnabled = enabled;
	}

	/**
	 * Set additional tags for the Micrometer listener timers.
	 * @param tags the tags.
	 * @since 2.4.6
	 */
	public void setMicrometerTags(Map<String, String> tags) {
		this.micrometerTags.putAll(tags);
	}

	/**
	 * Set a {@link ContainerCustomizer} that is invoked after a container is created and
	 * configured to enable further customization of the container.
	 * @param customizer the customizer.
	 * @since 2.4.6
	 */
	public void setSMLCCustomizer(ContainerCustomizer<SimpleMessageListenerContainer> customizer) {
		this.smlcCustomizer = customizer;
	}

	/**
	 * Set a {@link ContainerCustomizer} that is invoked after a container is created and
	 * configured to enable further customization of the container.
	 * @param customizer the customizer.
	 * @since 2.4.6
	 */
	public void setDMLCCustomizer(ContainerCustomizer<DirectMessageListenerContainer> customizer) {
		this.dmlcCustomizer = customizer;
	}

	@Override
	public Class<?> getObjectType() {
		return this.listenerContainer == null
				? AbstractMessageListenerContainer.class
				: this.listenerContainer.getClass();
	}

	@Override
	protected AbstractMessageListenerContainer createInstance() { // NOSONAR complexity
		if (this.listenerContainer == null) {
			AbstractMessageListenerContainer container = createContainer();
			JavaUtils.INSTANCE
					.acceptIfNotNull(this.applicationContext, container::setApplicationContext)
					.acceptIfNotNull(this.beanName, container::setBeanName)
					.acceptIfNotNull(this.applicationEventPublisher, container::setApplicationEventPublisher)
					.acceptIfNotNull(this.channelTransacted, container::setChannelTransacted)
					.acceptIfNotNull(this.acknowledgeMode, container::setAcknowledgeMode)
					.acceptIfNotNull(this.queueNames, container::setQueueNames)
					.acceptIfNotNull(this.queues, container::setQueues)
					.acceptIfNotNull(this.exposeListenerChannel, container::setExposeListenerChannel)
					.acceptIfNotNull(this.messageListener, container::setMessageListener)
					.acceptIfNotNull(this.errorHandler, container::setErrorHandler)
					.acceptIfNotNull(this.deBatchingEnabled, container::setDeBatchingEnabled)
					.acceptIfNotNull(this.adviceChain, container::setAdviceChain)
					.acceptIfNotNull(this.afterReceivePostProcessors, container::setAfterReceivePostProcessors)
					.acceptIfNotNull(this.autoStartup, container::setAutoStartup)
					.acceptIfNotNull(this.phase, container::setPhase)
					.acceptIfNotNull(this.listenerId, container::setListenerId)
					.acceptIfNotNull(this.consumerTagStrategy, container::setConsumerTagStrategy)
					.acceptIfNotNull(this.consumerArgs, container::setConsumerArguments)
					.acceptIfNotNull(this.noLocal, container::setNoLocal)
					.acceptIfNotNull(this.exclusive, container::setExclusive)
					.acceptIfNotNull(this.defaultRequeueRejected, container::setDefaultRequeueRejected)
					.acceptIfNotNull(this.prefetchCount, container::setPrefetchCount)
					.acceptIfNotNull(this.globalQos, container::setGlobalQos)
					.acceptIfNotNull(this.shutdownTimeout, container::setShutdownTimeout)
					.acceptIfNotNull(this.idleEventInterval, container::setIdleEventInterval)
					.acceptIfNotNull(this.transactionManager, container::setTransactionManager)
					.acceptIfNotNull(this.transactionAttribute, container::setTransactionAttribute)
					.acceptIfNotNull(this.taskExecutor, container::setTaskExecutor)
					.acceptIfNotNull(this.recoveryInterval, container::setRecoveryInterval)
					.acceptIfNotNull(this.recoveryBackOff, container::setRecoveryBackOff)
					.acceptIfNotNull(this.messagePropertiesConverter, container::setMessagePropertiesConverter)
					.acceptIfNotNull(this.rabbitAdmin, container::setAmqpAdmin)
					.acceptIfNotNull(this.missingQueuesFatal, container::setMissingQueuesFatal)
					.acceptIfNotNull(this.possibleAuthenticationFailureFatal,
							container::setPossibleAuthenticationFailureFatal)
					.acceptIfNotNull(this.mismatchedQueuesFatal, container::setMismatchedQueuesFatal)
					.acceptIfNotNull(this.autoDeclare, container::setAutoDeclare)
					.acceptIfNotNull(this.failedDeclarationRetryInterval, container::setFailedDeclarationRetryInterval)
					.acceptIfNotNull(this.exclusiveConsumerExceptionLogger,
							container::setExclusiveConsumerExceptionLogger)
					.acceptIfNotNull(this.micrometerEnabled, container::setMicrometerEnabled)
					.acceptIfCondition(!this.micrometerTags.isEmpty(), this.micrometerTags,
							container::setMicrometerTags);
			if (this.smlcCustomizer != null && this.type.equals(Type.simple)) {
				this.smlcCustomizer.configure((SimpleMessageListenerContainer) container);
			}
			else if (this.dmlcCustomizer != null && this.type.equals(Type.direct)) {
				this.dmlcCustomizer.configure((DirectMessageListenerContainer) container);
			}
			container.afterPropertiesSet();
			this.listenerContainer = container;
		}
		return this.listenerContainer;
	}

	private AbstractMessageListenerContainer createContainer() {
		Assert.notNull(this.connectionFactory, "'connectionFactory' is required");
		if (this.type.equals(Type.simple)) {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactory);
			JavaUtils.INSTANCE
					.acceptIfNotNull(this.concurrentConsumers, container::setConcurrentConsumers)
					.acceptIfNotNull(this.maxConcurrentConsumers, container::setMaxConcurrentConsumers)
					.acceptIfNotNull(this.startConsumerMinInterval, container::setStartConsumerMinInterval)
					.acceptIfNotNull(this.stopConsumerMinInterval, container::setStopConsumerMinInterval)
					.acceptIfNotNull(this.consecutiveActiveTrigger, container::setConsecutiveActiveTrigger)
					.acceptIfNotNull(this.consecutiveIdleTrigger, container::setConsecutiveIdleTrigger)
					.acceptIfNotNull(this.receiveTimeout, container::setReceiveTimeout)
					.acceptIfNotNull(this.batchReceiveTimeout, container::setBatchReceiveTimeout)
					.acceptIfNotNull(this.batchSize, container::setBatchSize)
					.acceptIfNotNull(this.consumerBatchEnabled, container::setConsumerBatchEnabled)
					.acceptIfNotNull(this.declarationRetries, container::setDeclarationRetries)
					.acceptIfNotNull(this.retryDeclarationInterval, container::setRetryDeclarationInterval);
			return container;
		}

		DirectMessageListenerContainer container = new DirectMessageListenerContainer(this.connectionFactory);
		JavaUtils.INSTANCE
				.acceptIfNotNull(this.consumersPerQueue, container::setConsumersPerQueue)
				.acceptIfNotNull(this.taskScheduler, container::setTaskScheduler)
				.acceptIfNotNull(this.monitorInterval, container::setMonitorInterval);
		return container;
	}

	@Override
	public void start() {
		if (this.listenerContainer != null) {
			this.listenerContainer.start();
		}
	}

	@Override
	public void stop() {
		if (this.listenerContainer != null) {
			this.listenerContainer.stop();
		}
	}

	@Override
	public boolean isRunning() {
		return this.listenerContainer != null && this.listenerContainer.isRunning();
	}

	@Override
	public int getPhase() {
		return (this.listenerContainer != null) ? this.listenerContainer.getPhase() : 0;
	}

	@Override
	public boolean isAutoStartup() {
		return this.listenerContainer != null && this.listenerContainer.isAutoStartup();
	}

	@Override
	public void stop(Runnable callback) {
		if (this.listenerContainer != null) {
			this.listenerContainer.stop(callback);
		}
		else {
			callback.run();
		}
	}

	/**
	 * The container type.
	 */
	public enum Type {

		/**
		 * {@link SimpleMessageListenerContainer}.
		 */
		simple,

		/**
		 * {@link DirectMessageListenerContainer}.
		 */
		direct

	}

}
