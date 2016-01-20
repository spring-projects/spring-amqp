/*
 * Copyright 2014-2016 the original author or authors.
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

import java.util.concurrent.Executor;

import org.aopalliance.aop.Advice;

import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * A {@link RabbitListenerContainerFactory} implementation to build a regular
 * {@link SimpleMessageListenerContainer}.
 *
 * <p>This should be the default for most users and a good transition paths
 * for those that are used to build such container definition manually.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.4
 */
public class SimpleRabbitListenerContainerFactory
		extends AbstractRabbitListenerContainerFactory<SimpleMessageListenerContainer>
		implements ApplicationContextAware, ApplicationEventPublisherAware {

	private Executor taskExecutor;

	private PlatformTransactionManager transactionManager;

	private Integer txSize;

	private Integer concurrentConsumers;

	private Integer maxConcurrentConsumers;

	private Long startConsumerMinInterval;

	private Long stopConsumerMinInterval;

	private Integer consecutiveActiveTrigger;

	private Integer consecutiveIdleTrigger;

	private Integer prefetchCount;

	private Long receiveTimeout;

	private Boolean defaultRequeueRejected;

	private Advice[] adviceChain;

	private BackOff recoveryBackOff;

	private Boolean missingQueuesFatal;

	private Boolean mismatchedQueuesFatal;

	private ConsumerTagStrategy consumerTagStrategy;

	private Long idleEventInterval;

	private ApplicationEventPublisher applicationEventPublisher;

	private ApplicationContext applicationContext;

	/**
	 * @param taskExecutor the {@link Executor} to use.
	 * @see SimpleMessageListenerContainer#setTaskExecutor
	 */
	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * @param transactionManager the {@link PlatformTransactionManager} to use.
	 * @see SimpleMessageListenerContainer#setTransactionManager
	 */
	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	/**
	 * @param txSize the transaction size.
	 * @see SimpleMessageListenerContainer#setTxSize
	 */
	public void setTxSize(Integer txSize) {
		this.txSize = txSize;
	}

	/**
	 * @param concurrency the minimum number of consumers to create.
	 * @see SimpleMessageListenerContainer#setConcurrentConsumers
	 */
	public void setConcurrentConsumers(Integer concurrency) {
		this.concurrentConsumers = concurrency;
	}

	/**
	 * @param maxConcurrency the maximum number of consumers.
	 * @see SimpleMessageListenerContainer#setMaxConcurrentConsumers
	 */
	public void setMaxConcurrentConsumers(Integer maxConcurrency) {
		this.maxConcurrentConsumers = maxConcurrency;
	}

	/**
	 * @param minStartInterval The minimum interval between new consumer starts.
	 * @see SimpleMessageListenerContainer#setStartConsumerMinInterval
	 */
	public void setStartConsumerMinInterval(Long minStartInterval) {
		this.startConsumerMinInterval = minStartInterval;
	}

	/**
	 * @param minStopInterval The minimum interval between consumer stops.
	 * @see SimpleMessageListenerContainer#setStopConsumerMinInterval
	 */
	public void setStopConsumerMinInterval(Long minStopInterval) {
		this.stopConsumerMinInterval = minStopInterval;
	}

	/**
	 * @param minConsecutiveActive The number of consecutive receives to trigger a new consumer.
	 * @see SimpleMessageListenerContainer#setConsecutiveActiveTrigger
	 */
	public void setConsecutiveActiveTrigger(Integer minConsecutiveActive) {
		this.consecutiveActiveTrigger = minConsecutiveActive;
	}

	/**
	 * @param minConsecutiveIdle The number of consecutive timeouts to trigger stopping a consumer.
	 * @see SimpleMessageListenerContainer#setConsecutiveIdleTrigger
	 */
	public void setConsecutiveIdleTrigger(Integer minConsecutiveIdle) {
		this.consecutiveIdleTrigger = minConsecutiveIdle;
	}

	/**
	 * @param prefetch the prefetch count
	 * @see SimpleMessageListenerContainer#setPrefetchCount(int)
	 */
	public void setPrefetchCount(Integer prefetch) {
		this.prefetchCount = prefetch;
	}

	/**
	 * @param receiveTimeout the timeout.
	 * @see SimpleMessageListenerContainer#setReceiveTimeout
	 */
	public void setReceiveTimeout(Long receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	/**
	 * @param requeueRejected true to reject by default.
	 * @see SimpleMessageListenerContainer#setDefaultRequeueRejected
	 */
	public void setDefaultRequeueRejected(Boolean requeueRejected) {
		this.defaultRequeueRejected = requeueRejected;
	}

	/**
	 * @param adviceChain the advice chain to set.
	 * @see SimpleMessageListenerContainer#setAdviceChain
	 */
	public void setAdviceChain(Advice... adviceChain) {
		this.adviceChain = adviceChain;
	}

	/**
	 * @param recoveryInterval The recovery interval.
	 * @see SimpleMessageListenerContainer#setRecoveryInterval
	 */
	public void setRecoveryInterval(Long recoveryInterval) {
		this.recoveryBackOff = new FixedBackOff(recoveryInterval, FixedBackOff.UNLIMITED_ATTEMPTS);
	}

	/**
	 * @param recoveryBackOff The BackOff to recover.
	 * @since 1.5
	 * @see SimpleMessageListenerContainer#setRecoveryBackOff(BackOff)
	 */
	public void setRecoveryBackOff(BackOff recoveryBackOff) {
		this.recoveryBackOff = recoveryBackOff;
	}

	/**
	 * @param missingQueuesFatal the missingQueuesFatal to set.
	 * @see SimpleMessageListenerContainer#setMissingQueuesFatal
	 */
	public void setMissingQueuesFatal(Boolean missingQueuesFatal) {
		this.missingQueuesFatal = missingQueuesFatal;
	}

	/**
	 * @param mismatchedQueuesFatal the mismatchedQueuesFatal to set.
	 * @since 1.6
	 * @see SimpleMessageListenerContainer#setMismatchedQueuesFatal(boolean)
	 */
	public void setMismatchedQueuesFatal(Boolean mismatchedQueuesFatal) {
		this.mismatchedQueuesFatal = mismatchedQueuesFatal;
	}

	/**
	 * @param consumerTagStrategy the consumerTagStrategy to set
	 * @see SimpleMessageListenerContainer#setConsumerTagStrategy(ConsumerTagStrategy)
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

	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	protected SimpleMessageListenerContainer createContainerInstance() {
		return new SimpleMessageListenerContainer();
	}

	@Override
	protected void initializeContainer(SimpleMessageListenerContainer instance) {
		super.initializeContainer(instance);

		if (this.applicationContext != null) {
			instance.setApplicationContext(this.applicationContext);
		}
		if (this.taskExecutor != null) {
			instance.setTaskExecutor(this.taskExecutor);
		}
		if (this.transactionManager != null) {
			instance.setTransactionManager(this.transactionManager);
		}
		if (this.txSize != null) {
			instance.setTxSize(this.txSize);
		}
		if (this.concurrentConsumers != null) {
			instance.setConcurrentConsumers(this.concurrentConsumers);
		}
		if (this.maxConcurrentConsumers != null) {
			instance.setMaxConcurrentConsumers(this.maxConcurrentConsumers);
		}
		if (this.startConsumerMinInterval != null) {
			instance.setStartConsumerMinInterval(this.startConsumerMinInterval);
		}
		if (this.stopConsumerMinInterval != null) {
			instance.setStopConsumerMinInterval(this.stopConsumerMinInterval);
		}
		if (this.consecutiveActiveTrigger != null) {
			instance.setConsecutiveActiveTrigger(this.consecutiveActiveTrigger);
		}
		if (this.consecutiveIdleTrigger != null) {
			instance.setConsecutiveIdleTrigger(this.consecutiveIdleTrigger);
		}
		if (this.prefetchCount != null) {
			instance.setPrefetchCount(this.prefetchCount);
		}
		if (this.receiveTimeout != null) {
			instance.setReceiveTimeout(this.receiveTimeout);
		}
		if (this.defaultRequeueRejected != null) {
			instance.setDefaultRequeueRejected(this.defaultRequeueRejected);
		}
		if (this.adviceChain != null) {
			instance.setAdviceChain(this.adviceChain);
		}
		if (this.recoveryBackOff != null) {
			instance.setRecoveryBackOff(this.recoveryBackOff);
		}
		if (this.mismatchedQueuesFatal != null) {
			instance.setMismatchedQueuesFatal(this.mismatchedQueuesFatal);
		}
		if (this.missingQueuesFatal != null) {
			instance.setMissingQueuesFatal(this.missingQueuesFatal);
		}
		if (this.consumerTagStrategy != null) {
			instance.setConsumerTagStrategy(this.consumerTagStrategy);
		}
		if (this.idleEventInterval != null) {
			instance.setIdleEventInterval(this.idleEventInterval);
		}
		if (this.applicationEventPublisher != null) {
			instance.setApplicationEventPublisher(this.applicationEventPublisher);
		}
	}

}
