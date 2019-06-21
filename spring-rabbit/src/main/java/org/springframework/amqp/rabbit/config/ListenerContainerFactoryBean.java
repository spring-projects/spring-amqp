/*
 * Copyright 2016-2019 the original author or authors.
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

import java.util.Map;
import java.util.concurrent.Executor;

import org.aopalliance.aop.Advice;

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
import org.springframework.amqp.support.converter.MessageConverter;
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
import org.springframework.util.ErrorHandler;
import org.springframework.util.backoff.BackOff;

/**
 * A Factory bean to create a listener container.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Johno Crawford
 *
 * @since 2.0
 *
 */
public class ListenerContainerFactoryBean extends AbstractFactoryBean<AbstractMessageListenerContainer>
		implements ApplicationContextAware, BeanNameAware, ApplicationEventPublisherAware, SmartLifecycle {

	private ApplicationContext applicationContext;

	private String beanName;

	private ApplicationEventPublisher applicationEventPublisher;

	private Type type = Type.simple;

	private AbstractMessageListenerContainer listenerContainer;

	private ConnectionFactory connectionFactory;

	private Boolean channelTransacted;

	private AcknowledgeMode acknowledgeMode;

	private String[] queueNames;

	private Queue[] queues;

	private Boolean exposeListenerChannel;

	private MessageListener messageListener;

	private ErrorHandler errorHandler;

	private MessageConverter messageConverter;

	private Boolean deBatchingEnabled;

	private Advice[] adviceChain;

	private MessagePostProcessor[] afterReceivePostProcessors;

	private Boolean autoStartup;

	private Integer phase;

	private String listenerId;

	private ConsumerTagStrategy consumerTagStrategy;

	private Map<String, Object> consumerArgs;

	private Boolean noLocal;

	private Boolean exclusive;

	private Boolean defaultRequeueRejected;

	private Integer prefetchCount;

	private Long shutdownTimeout;

	private Long idleEventInterval;

	private PlatformTransactionManager transactionManager;

	private TransactionAttribute transactionAttribute;

	private Executor taskExecutor;

	private Long recoveryInterval;

	private BackOff recoveryBackOff;

	private MessagePropertiesConverter messagePropertiesConverter;

	private RabbitAdmin rabbitAdmin;

	private Boolean missingQueuesFatal;

	private Boolean possibleAuthenticationFailureFatal;

	private Boolean mismatchedQueuesFatal;

	private Boolean autoDeclare;

	private Long failedDeclarationRetryInterval;

	private ConditionalExceptionLogger exclusiveConsumerExceptionLogger;

	private Integer consumersPerQueue;

	private TaskScheduler taskScheduler;

	private Long monitorInterval;

	private Integer concurrentConsumers;

	private Integer maxConcurrentConsumers;

	private Long startConsumerMinInterval;

	private Long stopConsumerMinInterval;

	private Integer consecutiveActiveTrigger;

	private Integer consecutiveIdleTrigger;

	private Long receiveTimeout;

	private Integer txSize;

	private Integer declarationRetries;

	private Long retryDeclarationInterval;

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

	public void setQueueNames(String... queueName) {
		this.queueNames = queueName;
	}

	public void setQueues(Queue... queues) {
		this.queues = queues;
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

	/*
	 * Unlikely this FB is used for a RabbitListener (it's only used by the
	 * XML parser and this property is never set). We could probably just
	 * remove this, but deprecating, just in case.
	 */
	/**
	 * Set the {@link MessageConverter} strategy for converting AMQP Messages.
	 * @param messageConverter the message converter to use
	 * @deprecated - this converter is not used by the container; it was only
	 * used to configure the converter for a {@code @RabbitListener} adapter.
	 * That is now handled differently. If you are manually creating a listener
	 * container, the converter must be configured in a listener adapter (if
	 * present).
	 */
	@Deprecated
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	public void setDeBatchingEnabled(boolean deBatchingEnabled) {
		this.deBatchingEnabled = deBatchingEnabled;
	}

	public void setAdviceChain(Advice... adviceChain) {
		this.adviceChain = adviceChain;
	}

	public void setAfterReceivePostProcessors(MessagePostProcessor... afterReceivePostProcessors) {
		this.afterReceivePostProcessors = afterReceivePostProcessors;
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

	public void setTxSize(int txSize) {
		this.txSize = txSize;
	}

	public void setDeclarationRetries(int declarationRetries) {
		this.declarationRetries = declarationRetries;
	}

	public void setRetryDeclarationInterval(long retryDeclarationInterval) {
		this.retryDeclarationInterval = retryDeclarationInterval;
	}

	@Override
	public Class<?> getObjectType() {
		return this.listenerContainer == null ? AbstractMessageListenerContainer.class : this.listenerContainer.getClass();
	}

	@SuppressWarnings("deprecation")
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
				.acceptIfNotNull(this.messageConverter, container::setMessageConverter)
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
				.acceptIfNotNull(this.possibleAuthenticationFailureFatal, container::setPossibleAuthenticationFailureFatal)
				.acceptIfNotNull(this.mismatchedQueuesFatal, container::setMismatchedQueuesFatal)
				.acceptIfNotNull(this.autoDeclare, container::setAutoDeclare)
				.acceptIfNotNull(this.failedDeclarationRetryInterval, container::setFailedDeclarationRetryInterval)
				.acceptIfNotNull(this.exclusiveConsumerExceptionLogger, container::setExclusiveConsumerExceptionLogger);
			container.afterPropertiesSet();
			this.listenerContainer = container;
		}
		return this.listenerContainer;
	}

	private AbstractMessageListenerContainer createContainer() {
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
				.acceptIfNotNull(this.txSize, container::setTxSize)
				.acceptIfNotNull(this.declarationRetries, container::setDeclarationRetries)
				.acceptIfNotNull(this.retryDeclarationInterval, container::setRetryDeclarationInterval);
			return container;
		}
		else {
			DirectMessageListenerContainer container = new DirectMessageListenerContainer(this.connectionFactory);
			JavaUtils.INSTANCE
				.acceptIfNotNull(this.consumersPerQueue, container::setConsumersPerQueue)
				.acceptIfNotNull(this.taskScheduler, container::setTaskScheduler)
				.acceptIfNotNull(this.monitorInterval, container::setMonitorInterval);
			return container;
		}
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
