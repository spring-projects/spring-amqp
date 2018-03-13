/*
 * Copyright 2014-2018 the original author or authors.
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
import java.util.concurrent.atomic.AtomicInteger;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.AbstractAdaptableMessageListener;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.transaction.PlatformTransactionManager;
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

	protected final Log logger = LogFactory.getLog(getClass());

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

	protected final AtomicInteger counter = new AtomicInteger();

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
	 * @see AbstractMessageListenerContainer#setMessageConverter(MessageConverter)
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
	 * @param prefetch the prefetch count
	 * @see SimpleMessageListenerContainer#setPrefetchCount(int)
	 */
	public void setPrefetchCount(Integer prefetch) {
		this.prefetchCount = prefetch;
	}

	/**
	 * @param requeueRejected true to reject by default.
	 * @see SimpleMessageListenerContainer#setDefaultRequeueRejected
	 */
	public void setDefaultRequeueRejected(Boolean requeueRejected) {
		this.defaultRequeueRejected = requeueRejected;
	}

	/**
	 * @return the advice chain that was set. Defaults to {@code null}.
	 * @since 1.7.4
	 */
	public Advice[] getAdviceChain() {
		return this.adviceChain;
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
	 * @param afterReceivePostProcessors the post processors.
	 * @since 2.0
	 * @see AbstractMessageListenerContainer#setAfterReceivePostProcessors(MessagePostProcessor...)
	 */
	public void setAfterReceivePostProcessors(MessagePostProcessor... afterReceivePostProcessors) {
		this.afterReceivePostProcessors = afterReceivePostProcessors;
	}

	/**
	 * Set post processors that will be applied before sending replies.
	 * @param beforeSendReplyPostProcessors the post processors.
	 * @since 2.0.3
	 */
	public void setBeforeSendReplyPostProcessors(MessagePostProcessor... beforeSendReplyPostProcessors) {
		this.beforeSendReplyPostProcessors = beforeSendReplyPostProcessors;
	}

	@Override
	public C createListenerContainer(RabbitListenerEndpoint endpoint) {
		C instance = createContainerInstance();

		if (this.connectionFactory != null) {
			instance.setConnectionFactory(this.connectionFactory);
		}
		if (this.errorHandler != null) {
			instance.setErrorHandler(this.errorHandler);
		}
		if (this.messageConverter != null) {
			instance.setMessageConverter(this.messageConverter);
		}
		if (this.acknowledgeMode != null) {
			instance.setAcknowledgeMode(this.acknowledgeMode);
		}
		if (this.channelTransacted != null) {
			instance.setChannelTransacted(this.channelTransacted);
		}
		if (this.applicationContext != null) {
			instance.setApplicationContext(this.applicationContext);
		}
		if (this.taskExecutor != null) {
			instance.setTaskExecutor(this.taskExecutor);
		}
		if (this.transactionManager != null) {
			instance.setTransactionManager(this.transactionManager);
		}
		if (this.prefetchCount != null) {
			instance.setPrefetchCount(this.prefetchCount);
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
		if (this.failedDeclarationRetryInterval != null) {
			instance.setFailedDeclarationRetryInterval(this.failedDeclarationRetryInterval);
		}
		if (this.applicationEventPublisher != null) {
			instance.setApplicationEventPublisher(this.applicationEventPublisher);
		}
		if (endpoint.getAutoStartup() != null) {
			instance.setAutoStartup(endpoint.getAutoStartup());
		}
		else if (this.autoStartup != null) {
			instance.setAutoStartup(this.autoStartup);
		}
		if (this.phase != null) {
			instance.setPhase(this.phase);
		}
		if (this.afterReceivePostProcessors != null) {
			instance.setAfterReceivePostProcessors(this.afterReceivePostProcessors);
		}
		instance.setListenerId(endpoint.getId());

		endpoint.setupListenerContainer(instance);
		if (this.beforeSendReplyPostProcessors != null
				&& instance.getMessageListener() instanceof AbstractAdaptableMessageListener) {
			((AbstractAdaptableMessageListener) instance.getMessageListener())
					.setBeforeSendReplyPostProcessors(this.beforeSendReplyPostProcessors);
		}
		initializeContainer(instance, endpoint);

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
