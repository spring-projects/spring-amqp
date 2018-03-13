/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

	private AbstractMessageListenerContainer container;

	private ConnectionFactory connectionFactory;

	private Boolean channelTransacted;

	private AcknowledgeMode acknowledgeMode;

	private String[] queueNames;

	private Queue[] queues;

	private Boolean exposeListenerChannel;

	private Object messageListener;

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

	public void setMessageListener(Object messageListener) {
		this.messageListener = messageListener;
	}

	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

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
		return this.container == null ? AbstractMessageListenerContainer.class : this.container.getClass();
	}

	@Override
	protected AbstractMessageListenerContainer createInstance() throws Exception {
		if (this.container == null) {
			AbstractMessageListenerContainer container = createContainer();
			if (this.applicationContext != null) {
				container.setApplicationContext(this.applicationContext);
			}
			if (this.beanName != null) {
				container.setBeanName(this.beanName);
			}
			if (this.applicationEventPublisher != null) {
				container.setApplicationEventPublisher(this.applicationEventPublisher);
			}
			if (this.channelTransacted != null) {
				container.setChannelTransacted(this.channelTransacted);
			}
			if (this.acknowledgeMode != null) {
				container.setAcknowledgeMode(this.acknowledgeMode);
			}
			if (this.queueNames != null) {
				container.setQueueNames(this.queueNames);
			}
			if (this.queues != null) {
				container.setQueues(this.queues);
			}
			if (this.exposeListenerChannel != null) {
				container.setExposeListenerChannel(this.exposeListenerChannel);
			}
			if (this.messageListener != null) {
				container.setMessageListener(this.messageListener);
			}
			if (this.errorHandler != null) {
				container.setErrorHandler(this.errorHandler);
			}
			if (this.messageConverter != null) {
				container.setMessageConverter(this.messageConverter);
			}
			if (this.deBatchingEnabled != null) {
				container.setDeBatchingEnabled(this.deBatchingEnabled);
			}
			if (this.adviceChain != null) {
				container.setAdviceChain(this.adviceChain);
			}
			if (this.afterReceivePostProcessors != null) {
				container.setAfterReceivePostProcessors(this.afterReceivePostProcessors);
			}
			if (this.autoStartup != null) {
				container.setAutoStartup(this.autoStartup);
			}
			if (this.phase != null) {
				container.setPhase(this.phase);
			}
			if (this.listenerId != null) {
				container.setListenerId(this.listenerId);
			}
			if (this.consumerTagStrategy != null) {
				container.setConsumerTagStrategy(this.consumerTagStrategy);
			}
			if (this.consumerArgs != null) {
				container.setConsumerArguments(this.consumerArgs);
			}
			if (this.noLocal != null) {
				container.setNoLocal(this.noLocal);
			}
			if (this.exclusive != null) {
				container.setExclusive(this.exclusive);
			}
			if (this.defaultRequeueRejected != null) {
				container.setDefaultRequeueRejected(this.defaultRequeueRejected);
			}
			if (this.prefetchCount != null) {
				container.setPrefetchCount(this.prefetchCount);
			}
			if (this.shutdownTimeout != null) {
				container.setShutdownTimeout(this.shutdownTimeout);
			}
			if (this.idleEventInterval != null) {
				container.setIdleEventInterval(this.idleEventInterval);
			}
			if (this.transactionManager != null) {
				container.setTransactionManager(this.transactionManager);
			}
			if (this.transactionAttribute != null) {
				container.setTransactionAttribute(this.transactionAttribute);
			}
			if (this.taskExecutor != null) {
				container.setTaskExecutor(this.taskExecutor);
			}
			if (this.recoveryInterval != null) {
				container.setRecoveryInterval(this.recoveryInterval);
			}
			if (this.recoveryBackOff != null) {
				container.setRecoveryBackOff(this.recoveryBackOff);
			}
			if (this.messagePropertiesConverter != null) {
				container.setMessagePropertiesConverter(this.messagePropertiesConverter);
			}
			if (this.rabbitAdmin != null) {
				container.setAmqpAdmin(this.rabbitAdmin);
			}
			if (this.missingQueuesFatal != null) {
				container.setMissingQueuesFatal(this.missingQueuesFatal);
			}
			if (this.possibleAuthenticationFailureFatal != null) {
				container.setPossibleAuthenticationFailureFatal(this.possibleAuthenticationFailureFatal);
			}
			if (this.mismatchedQueuesFatal != null) {
				container.setMismatchedQueuesFatal(this.mismatchedQueuesFatal);
			}
			if (this.autoDeclare != null) {
				container.setAutoDeclare(this.autoDeclare);
			}
			if (this.failedDeclarationRetryInterval != null) {
				container.setFailedDeclarationRetryInterval(this.failedDeclarationRetryInterval);
			}
			if (this.exclusiveConsumerExceptionLogger != null) {
				container.setExclusiveConsumerExceptionLogger(this.exclusiveConsumerExceptionLogger);
			}
			container.afterPropertiesSet();
			this.container = container;
		}
		return this.container;
	}

	private AbstractMessageListenerContainer createContainer() {
		if (this.type.equals(Type.simple)) {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactory);
			if (this.concurrentConsumers != null) {
				container.setConcurrentConsumers(this.concurrentConsumers);
			}
			if (this.maxConcurrentConsumers != null) {
				container.setMaxConcurrentConsumers(this.maxConcurrentConsumers);
			}
			if (this.startConsumerMinInterval != null) {
				container.setStartConsumerMinInterval(this.startConsumerMinInterval);
			}
			if (this.stopConsumerMinInterval != null) {
				container.setStopConsumerMinInterval(this.stopConsumerMinInterval);
			}
			if (this.consecutiveActiveTrigger != null) {
				container.setConsecutiveActiveTrigger(this.consecutiveActiveTrigger);
			}
			if (this.consecutiveIdleTrigger != null) {
				container.setConsecutiveIdleTrigger(this.consecutiveIdleTrigger);
			}
			if (this.receiveTimeout != null) {
				container.setReceiveTimeout(this.receiveTimeout);
			}
			if (this.txSize != null) {
				container.setTxSize(this.txSize);
			}
			if (this.declarationRetries != null) {
				container.setDeclarationRetries(this.declarationRetries);
			}
			if (this.retryDeclarationInterval != null) {
				container.setRetryDeclarationInterval(this.retryDeclarationInterval);
			}
			return container;
		}
		else {
			DirectMessageListenerContainer container = new DirectMessageListenerContainer(this.connectionFactory);
			if (this.consumersPerQueue != null) {
				container.setConsumersPerQueue(this.consumersPerQueue);
			}
			if (this.taskScheduler != null) {
				container.setTaskScheduler(this.taskScheduler);
			}
			if (this.monitorInterval != null) {
				container.setMonitorInterval(this.monitorInterval);
			}
			return container;
		}
	}

	@Override
	public void start() {
		if (this.container != null) {
			this.container.start();
		}
	}

	@Override
	public void stop() {
		if (this.container != null) {
			this.container.stop();
		}
	}

	@Override
	public boolean isRunning() {
		return this.container != null && this.container.isRunning();
	}

	@Override
	public int getPhase() {
		return (this.container != null) ? this.container.getPhase() : 0;
	}

	@Override
	public boolean isAutoStartup() {
		return this.container != null && this.container.isAutoStartup();
	}

	@Override
	public void stop(Runnable callback) {
		if (this.container != null) {
			this.container.stop(callback);
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
