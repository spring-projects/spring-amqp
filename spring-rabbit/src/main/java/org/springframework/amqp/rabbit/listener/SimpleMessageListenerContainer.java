/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.ConsumerChannelRegistry;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerExecutionException;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.support.ConsumerCancelledException;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.ListenerContainerAware;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.ConditionalExceptionLogger;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.aop.Pointcut;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.support.MetricType;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.0
 */
public class SimpleMessageListenerContainer extends AbstractMessageListenerContainer
		implements ApplicationEventPublisherAware {

	private static final long DEFAULT_START_CONSUMER_MIN_INTERVAL = 10000;

	private static final long DEFAULT_STOP_CONSUMER_MIN_INTERVAL = 60000;

	private static final int DEFAULT_CONSECUTIVE_ACTIVE_TRIGGER = 10;

	private static final int DEFAULT_CONSECUTIVE_IDLE_TRIGGER = 10;

	public static final long DEFAULT_RECEIVE_TIMEOUT = 1000;

	public static final int DEFAULT_PREFETCH_COUNT = 1;

	public static final long DEFAULT_SHUTDOWN_TIMEOUT = 5000;

	/**
	 * The default recovery interval: 5000 ms = 5 seconds.
	 */
	public static final long DEFAULT_RECOVERY_INTERVAL = 5000;

	private final AtomicLong lastNoMessageAlert = new AtomicLong();

	private volatile int prefetchCount = DEFAULT_PREFETCH_COUNT;

	private volatile long startConsumerMinInterval = DEFAULT_START_CONSUMER_MIN_INTERVAL;

	private volatile long stopConsumerMinInterval = DEFAULT_STOP_CONSUMER_MIN_INTERVAL;

	private volatile int consecutiveActiveTrigger = DEFAULT_CONSECUTIVE_ACTIVE_TRIGGER;

	private volatile int consecutiveIdleTrigger = DEFAULT_CONSECUTIVE_IDLE_TRIGGER;

	private volatile int txSize = 1;

	private volatile Executor taskExecutor = new SimpleAsyncTaskExecutor();

	private volatile boolean taskExecutorSet;

	private volatile int concurrentConsumers = 1;

	private volatile Integer maxConcurrentConsumers;

	private volatile boolean exclusive;

	private volatile long lastConsumerStarted;

	private volatile long lastConsumerStopped;

	private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

	private volatile long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

	private BackOff recoveryBackOff = new FixedBackOff(DEFAULT_RECOVERY_INTERVAL, FixedBackOff.UNLIMITED_ATTEMPTS);

	// Map entry value, when false, signals the consumer to terminate
	private Map<BlockingQueueConsumer, Boolean> consumers;

	private final Object consumersMonitor = new Object();

	private PlatformTransactionManager transactionManager;

	private TransactionAttribute transactionAttribute = new DefaultTransactionAttribute();

	private volatile Advice[] adviceChain = new Advice[0];

	private final ActiveObjectCounter<BlockingQueueConsumer> cancellationLock = new ActiveObjectCounter<BlockingQueueConsumer>();

	private volatile MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private volatile boolean defaultRequeueRejected = true;

	private final Map<String, Object> consumerArgs = new HashMap<String, Object>();

	private volatile RabbitAdmin rabbitAdmin;

	private volatile boolean missingQueuesFatal = true;

	private volatile boolean missingQueuesFatalSet;

	private volatile boolean autoDeclare = true;

	private volatile boolean mismatchedQueuesFatal = false;

	private volatile ConsumerTagStrategy consumerTagStrategy;

	private volatile ApplicationEventPublisher applicationEventPublisher;

	public interface ContainerDelegate {
		void invokeListener(Channel channel, Message message) throws Exception;
	}

	private final ContainerDelegate delegate = new ContainerDelegate() {
		@Override
		public void invokeListener(Channel channel, Message message) throws Exception {
			SimpleMessageListenerContainer.super.invokeListener(channel, message);
		}
	};

	private ContainerDelegate proxy = this.delegate;

	private Integer declarationRetries;

	private Long failedDeclarationRetryInterval;

	private Long retryDeclarationInterval;

	private ConditionalExceptionLogger exclusiveConsumerExceptionLogger = new DefaultExclusiveConsumerLogger();

	private Long idleEventInterval;

	private volatile long lastReceive = System.currentTimeMillis();

	/**
	 * Default constructor for convenient dependency injection via setters.
	 */
	public SimpleMessageListenerContainer() {
	}

	/**
	 * Create a listener container from the connection factory (mandatory).
	 *
	 * @param connectionFactory the {@link ConnectionFactory}
	 */
	public SimpleMessageListenerContainer(ConnectionFactory connectionFactory) {
		this.setConnectionFactory(connectionFactory);
	}

	/**
	 * Public setter for the {@link Advice} to apply to listener executions. If {@link #setTxSize(int) txSize>1} then
	 * multiple listener executions will all be wrapped in the same advice up to that limit.
	 * <p>
	 * If a {@link #setTransactionManager(PlatformTransactionManager) transactionManager} is provided as well, then
	 * separate advice is created for the transaction and applied first in the chain. In that case the advice chain
	 * provided here should not contain a transaction interceptor (otherwise two transactions would be be applied).
	 * @param adviceChain the advice chain to set
	 */
	public void setAdviceChain(Advice[] adviceChain) {
		this.adviceChain = Arrays.copyOf(adviceChain, adviceChain.length);
	}

	/**
	 * Specify the interval between recovery attempts, in <b>milliseconds</b>.
	 * The default is 5000 ms, that is, 5 seconds.
	 * @param recoveryInterval The recovery interval.
	 */
	public void setRecoveryInterval(long recoveryInterval) {
		this.recoveryBackOff = new FixedBackOff(recoveryInterval, FixedBackOff.UNLIMITED_ATTEMPTS);
	}

	/**
	 * Specify the {@link BackOff} for interval between recovery attempts.
	 * The default is 5000 ms, that is, 5 seconds.
	 * With the {@link BackOff} you can supply the {@code maxAttempts} for recovery before
	 * the {@link #stop()} will be performed.
	 * @param recoveryBackOff The BackOff to recover.
	 * @since 1.5
	 */
	public void setRecoveryBackOff(BackOff recoveryBackOff) {
		Assert.notNull(recoveryBackOff, "'recoveryBackOff' must not be null.");
		this.recoveryBackOff = recoveryBackOff;
	}


	/**
	 * Specify the number of concurrent consumers to create. Default is 1.
	 * <p>
	 * Raising the number of concurrent consumers is recommended in order to scale the consumption of messages coming in
	 * from a queue. However, note that any ordering guarantees are lost once multiple consumers are registered. In
	 * general, stick with 1 consumer for low-volume queues. Cannot be more than {@link #maxConcurrentConsumers} (if set).
	 * @param concurrentConsumers the minimum number of consumers to create.
	 * @see #setMaxConcurrentConsumers(int)
	 */
	public void setConcurrentConsumers(final int concurrentConsumers) {
		Assert.isTrue(concurrentConsumers > 0, "'concurrentConsumers' value must be at least 1 (one)");
		Assert.isTrue(!this.exclusive || concurrentConsumers == 1,
				"When the consumer is exclusive, the concurrency must be 1");
		if (this.maxConcurrentConsumers != null) {
			Assert.isTrue(concurrentConsumers <= this.maxConcurrentConsumers,
					"'concurrentConsumers' cannot be more than 'maxConcurrentConsumers'");
		}
		synchronized(this.consumersMonitor) {
			if (logger.isDebugEnabled()) {
				logger.debug("Changing consumers from " + this.concurrentConsumers + " to " + concurrentConsumers);
			}
			int delta = this.concurrentConsumers - concurrentConsumers;
			this.concurrentConsumers = concurrentConsumers;
			if (isActive() && this.consumers != null) {
				if (delta > 0) {
					Iterator<Entry<BlockingQueueConsumer, Boolean>> entryIterator = this.consumers.entrySet()
							.iterator();
					while (entryIterator.hasNext() && delta > 0) {
						Entry<BlockingQueueConsumer, Boolean> entry = entryIterator.next();
						if (entry.getValue()) {
							BlockingQueueConsumer consumer = entry.getKey();
							consumer.basicCancel();
							this.consumers.put(consumer, false);
							delta--;
						}
					}

				}
				else {
					addAndStartConsumers(-delta);
				}
			}
		}
	}

	/**
	 * Sets an upper limit to the number of consumers; defaults to 'concurrentConsumers'. Consumers
	 * will be added on demand. Cannot be less than {@link #concurrentConsumers}.
	 * @param maxConcurrentConsumers the maximum number of consumers.
	 * @see #setConcurrentConsumers(int)
	 * @see #setStartConsumerMinInterval(long)
	 * @see #setStopConsumerMinInterval(long)
	 * @see #setConsecutiveActiveTrigger(int)
	 * @see #setConsecutiveIdleTrigger(int)
	 */
	public void setMaxConcurrentConsumers(int maxConcurrentConsumers) {
		Assert.isTrue(maxConcurrentConsumers >= this.concurrentConsumers,
				"'maxConcurrentConsumers' value must be at least 'concurrentConsumers'");
		Assert.isTrue(!this.exclusive || maxConcurrentConsumers == 1,
				"When the consumer is exclusive, the concurrency must be 1");
		this.maxConcurrentConsumers = maxConcurrentConsumers;
	}

	/**
	 * Set to true for an exclusive consumer - if true, the concurrency must be 1.
	 * @param exclusive true for an exclusive consumer.
	 */
	public final void setExclusive(boolean exclusive) {
		Assert.isTrue(!exclusive || (this.concurrentConsumers == 1
				&& (this.maxConcurrentConsumers == null || this.maxConcurrentConsumers == 1)),
				"When the consumer is exclusive, the concurrency must be 1");
		this.exclusive = exclusive;
	}

	/**
	 * If {@link #maxConcurrentConsumers} is greater then {@link #concurrentConsumers}, and
	 * {@link #maxConcurrentConsumers} has not been reached, specifies
	 * the minimum time (milliseconds) between starting new consumers on demand. Default is 10000
	 * (10 seconds).
	 * @param startConsumerMinInterval The minimum interval between new consumer starts.
	 * @see #setMaxConcurrentConsumers(int)
	 * @see #setStartConsumerMinInterval(long)
	 */
	public final void setStartConsumerMinInterval(long startConsumerMinInterval) {
		Assert.isTrue(startConsumerMinInterval > 0, "'startConsumerMinInterval' must be > 0");
		this.startConsumerMinInterval = startConsumerMinInterval;
	}

	/**
	 * If {@link #maxConcurrentConsumers} is greater then {@link #concurrentConsumers}, and
	 * the number of consumers exceeds {@link #concurrentConsumers}, specifies the
	 * minimum time (milliseconds) between stopping idle consumers. Default is 60000
	 * (1 minute).
	 * @param stopConsumerMinInterval The minimum interval between consumer stops.
	 * @see #setMaxConcurrentConsumers(int)
	 * @see #setStopConsumerMinInterval(long)
	 */
	public final void setStopConsumerMinInterval(long stopConsumerMinInterval) {
		Assert.isTrue(stopConsumerMinInterval > 0, "'stopConsumerMinInterval' must be > 0");
		this.stopConsumerMinInterval = stopConsumerMinInterval;
	}

	/**
	 * If {@link #maxConcurrentConsumers} is greater then {@link #concurrentConsumers}, and
	 * {@link #maxConcurrentConsumers} has not been reached, specifies the number of
	 * consecutive cycles when a single consumer was active, in order to consider
	 * starting a new consumer. If the consumer goes idle for one cycle, the counter is reset.
	 * This is impacted by the {@link #txSize}.
	 * Default is 10 consecutive messages.
	 * @param consecutiveActiveTrigger The number of consecutive receives to trigger a new consumer.
	 * @see #setMaxConcurrentConsumers(int)
	 * @see #setStartConsumerMinInterval(long)
	 * @see #setTxSize(int)
	 */
	public final void setConsecutiveActiveTrigger(int consecutiveActiveTrigger) {
		Assert.isTrue(consecutiveActiveTrigger > 0, "'consecutiveActiveTrigger' must be > 0");
		this.consecutiveActiveTrigger = consecutiveActiveTrigger;
	}

	/**
	 * If {@link #maxConcurrentConsumers} is greater then {@link #concurrentConsumers}, and
	 * the number of consumers exceeds {@link #concurrentConsumers}, specifies the
	 * number of consecutive receive attempts that return no data; after which we consider
	 * stopping a consumer. The idle time is effectively
	 * {@link #receiveTimeout} * {@link #txSize} * this value because the consumer thread waits for
	 * a message for up to {@link #receiveTimeout} up to {@link #txSize} times.
	 * Default is 10 consecutive idles.
	 * @param consecutiveIdleTrigger The number of consecutive timeouts to trigger stopping a consumer.
	 * @see #setMaxConcurrentConsumers(int)
	 * @see #setStopConsumerMinInterval(long)
	 * @see #setReceiveTimeout(long)
	 * @see #setTxSize(int)
	 */
	public final void setConsecutiveIdleTrigger(int consecutiveIdleTrigger) {
		Assert.isTrue(consecutiveIdleTrigger > 0, "'consecutiveIdleTrigger' must be > 0");
		this.consecutiveIdleTrigger = consecutiveIdleTrigger;
	}

	/**
	 * The time (in milliseconds) that a consumer should wait for data. Default
	 * 1000 (1 second).
	 * @param receiveTimeout the timeout.
	 * @see #setConsecutiveIdleTrigger(int)
	 */
	public void setReceiveTimeout(long receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	/**
	 * The time to wait for workers in milliseconds after the container is stopped, and before the connection is forced
	 * closed. If any workers are active when the shutdown signal comes they will be allowed to finish processing as
	 * long as they can finish within this timeout. Otherwise the connection is closed and messages remain unacked (if
	 * the channel is transactional). Defaults to 5 seconds.
	 * @param shutdownTimeout the shutdown timeout to set
	 */
	public void setShutdownTimeout(long shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}

	public void setTaskExecutor(Executor taskExecutor) {
		Assert.notNull(taskExecutor, "taskExecutor must not be null");
		this.taskExecutor = taskExecutor;
		this.taskExecutorSet = true;
	}

	/**
	 * Tells the broker how many messages to send to each consumer in a single request. Often this can be set quite high
	 * to improve throughput. It should be greater than or equal to {@link #setTxSize(int) the transaction size}.
	 * @param prefetchCount the prefetch count
	 */
	public void setPrefetchCount(int prefetchCount) {
		this.prefetchCount = prefetchCount;
	}

	/**
	 * Tells the container how many messages to process in a single transaction (if the channel is transactional). For
	 * best results it should be less than or equal to {@link #setPrefetchCount(int) the prefetch count}. Also affects
	 * how often acks are sent when using {@link AcknowledgeMode#AUTO} - one ack per txSize. Default is 1.
	 * @param txSize the transaction size
	 */
	public void setTxSize(int txSize) {
		Assert.isTrue(txSize > 0, "'txSize' must be > 0");
		this.txSize = txSize;
	}

	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	/**
	 * @param transactionAttribute the transaction attribute to set
	 */
	public void setTransactionAttribute(TransactionAttribute transactionAttribute) {
		this.transactionAttribute = transactionAttribute;
	}

	/**
	 * Set the {@link MessagePropertiesConverter} for this listener container.
	 * @param messagePropertiesConverter The properties converter.
	 */
	public void setMessagePropertiesConverter(MessagePropertiesConverter messagePropertiesConverter) {
		Assert.notNull(messagePropertiesConverter, "messagePropertiesConverter must not be null");
		this.messagePropertiesConverter = messagePropertiesConverter;
	}

	/**
	 * Determines the default behavior when a message is rejected, for example because the listener
	 * threw an exception. When true, messages will be requeued, when false, they will not. For
	 * versions of Rabbit that support dead-lettering, the message must not be requeued in order
	 * to be sent to the dead letter exchange. Setting to false causes all rejections to not
	 * be requeued. When true, the default can be overridden by the listener throwing an
	 * {@link AmqpRejectAndDontRequeueException}. Default true.
	 * @param defaultRequeueRejected true to reject by default.
	 */
	public void setDefaultRequeueRejected(boolean defaultRequeueRejected) {
		this.defaultRequeueRejected = defaultRequeueRejected;
	}

	public void setConsumerArguments(Map<String, Object> args) {
		synchronized(this.consumersMonitor) {
			this.consumerArgs.clear();
			this.consumerArgs.putAll(args);
		}
	}

	protected RabbitAdmin getRabbitAdmin() {
		return this.rabbitAdmin;
	}

	/**
	 * Set the {@link RabbitAdmin}, used to declare any auto-delete queues, bindings
	 * etc when the container is started. Only needed if those queues use conditional
	 * declaration (have a 'declared-by' attribute). If not specified, an internal
	 * admin will be used which will attempt to declare all elements not having a
	 * 'declared-by' attribute.
	 * @param rabbitAdmin The admin.
	 */
	public void setRabbitAdmin(RabbitAdmin rabbitAdmin) {
		this.rabbitAdmin = rabbitAdmin;
	}

	/**
	 * If all of the configured queue(s) are not available on the broker, this setting
	 * determines whether the condition is fatal (default true). When true, and
	 * the queues are missing during startup, the context refresh() will fail. If
	 * the queues are removed while the container is running, the container is
	 * stopped.
	 * <p> When false, the condition is not considered fatal and the container will
	 * continue to attempt to start the consumers according to the {@link #setRecoveryInterval(long)}.
	 * Note that each consumer will make 3 attempts (at 5 second intervals) on each
	 * recovery attempt.
	 * @param missingQueuesFatal the missingQueuesFatal to set.
	 * @since 1.3.5
	 */
	public void setMissingQueuesFatal(boolean missingQueuesFatal) {
		this.missingQueuesFatal = missingQueuesFatal;
		this.missingQueuesFatalSet = true;
	}

	/**
	 * Prevent the container from starting if any of the queues defined in the context have
	 * mismatched arguments (TTL etc). Default false.
	 * @param mismatchedQueuesFatal true to fail initialization when this condition occurs.
	 * @since 1.6
	 */
	public void setMismatchedQueuesFatal(boolean mismatchedQueuesFatal) {
		this.mismatchedQueuesFatal = mismatchedQueuesFatal;
	}

	/**
	 * {@inheritDoc}
	 * @since 1.5
	 */
	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	public void setQueueNames(String... queueName) {
		super.setQueueNames(queueName);
		this.queuesChanged();
	}

	@Override
	public void setQueues(Queue... queues) {
		super.setQueues(queues);
		this.queuesChanged();
	}

	/**
	 * @param autoDeclare the boolean flag to indicate an redeclaration operation.
	 * @since 1.4
	 * @see #redeclareElementsIfNecessary
	 */
	public void setAutoDeclare(boolean autoDeclare) {
		this.autoDeclare = autoDeclare;
	}

	/**
	 * Add queue(s) to this container's list of queues. The existing consumers
	 * will be cancelled after they have processed any pre-fetched messages and
	 * new consumers will be created. The queue must exist to avoid problems when
	 * restarting the consumers.
	 * @param queueName The queue to add.
	 */
	@Override
	public void addQueueNames(String... queueName) {
		super.addQueueNames(queueName);
		this.queuesChanged();
	}

	/**
	 * Add queue(s) to this container's list of queues. The existing consumers
	 * will be cancelled after they have processed any pre-fetched messages and
	 * new consumers will be created. The queue must exist to avoid problems when
	 * restarting the consumers.
	 * @param queue The queue to add.
	 */
	@Override
	public void addQueues(Queue... queue) {
		super.addQueues(queue);
		this.queuesChanged();
	}

	/**
	 * Remove queues from this container's list of queues. The existing consumers
	 * will be cancelled after they have processed any pre-fetched messages and
	 * new consumers will be created. At least one queue must remain.
	 * @param queueName The queue to remove.
	 */
	@Override
	public boolean removeQueueNames(String... queueName) {
		if (super.removeQueueNames(queueName)) {
			this.queuesChanged();
			return true;
		}
		else {
			return false;
		}
	}

	/**
	 * Remove queue(s) from this container's list of queues. The existing consumers
	 * will be cancelled after they have processed any pre-fetched messages and
	 * new consumers will be created. At least one queue must remain.
	 * @param queue The queue to remove.
	 */
	@Override
	public boolean removeQueues(Queue... queue) {
		if (super.removeQueues(queue)) {
			this.queuesChanged();
			return true;
		}
		else {
			return false;
		}
	}

	/**
	 * Set the number of retries after passive queue declaration fails.
	 * @param declarationRetries The number of retries, default 3.
	 * @see #setFailedDeclarationRetryInterval(long)
	 * @since 1.3.9
	 */
	public void setDeclarationRetries(int declarationRetries) {
		this.declarationRetries = declarationRetries;
	}

	/**
	 * Set the interval between passive queue declaration attempts in milliseconds.
	 * @param failedDeclarationRetryInterval the interval, default 5000.
	 * @see #setDeclarationRetries(int)
	 * @since 1.3.9
	 */
	public void setFailedDeclarationRetryInterval(long failedDeclarationRetryInterval) {
		this.failedDeclarationRetryInterval = failedDeclarationRetryInterval;
	}

	/**
	 * When consuming multiple queues, set the interval between declaration attempts when only
	 * a subset of the queues were available (milliseconds).
	 * @param retryDeclarationInterval the interval, default 60000.
	 * @since 1.3.9
	 */
	public void setRetryDeclarationInterval(long retryDeclarationInterval) {
		this.retryDeclarationInterval = retryDeclarationInterval;
	}

	/**
	 * Set the implementation of {@link ConsumerTagStrategy} to generate consumer tags.
	 * By default, the RabbitMQ server generates consumer tags.
	 * @param consumerTagStrategy the consumerTagStrategy to set.
	 * @since 1.4.5
	 */
	public void setConsumerTagStrategy(ConsumerTagStrategy consumerTagStrategy) {
		this.consumerTagStrategy = consumerTagStrategy;
	}

	/**
	 * Set a {@link ConditionalExceptionLogger} for logging exclusive consumer failures. The
	 * default is to log such failures at WARN level.
	 * @param exclusiveConsumerExceptionLogger the conditional exception logger.
	 * @since 1.5
	 */
	public void setExclusiveConsumerExceptionLogger(ConditionalExceptionLogger exclusiveConsumerExceptionLogger) {
		this.exclusiveConsumerExceptionLogger = exclusiveConsumerExceptionLogger;
	}

	/**
	 * How often to emit {@link ListenerContainerIdleEvent}s in milliseconds.
	 * @param idleEventInterval the interval.
	 */
	public void setIdleEventInterval(long idleEventInterval) {
		this.idleEventInterval = idleEventInterval;
	}

	/**
	 * Avoid the possibility of not configuring the CachingConnectionFactory in sync with the number of concurrent
	 * consumers.
	 */
	@Override
	protected void validateConfiguration() {

		super.validateConfiguration();

		Assert.state(
				!(getAcknowledgeMode().isAutoAck() && this.transactionManager != null),
				"The acknowledgeMode is NONE (autoack in Rabbit terms) which is not consistent with having an "
						+ "external transaction manager. Either use a different AcknowledgeMode or make sure " +
						"the transactionManager is null.");

		if (this.getConnectionFactory() instanceof CachingConnectionFactory) {
			CachingConnectionFactory cf = (CachingConnectionFactory) getConnectionFactory();
			if (cf.getCacheMode() == CacheMode.CHANNEL && cf.getChannelCacheSize() < this.concurrentConsumers) {
				cf.setChannelCacheSize(this.concurrentConsumers);
				logger.warn("CachingConnectionFactory's channelCacheSize can not be less than the number " +
						"of concurrentConsumers so it was reset to match: "	+ this.concurrentConsumers);
			}
		}

	}

	private void initializeProxy() {
		if (this.adviceChain.length == 0) {
			return;
		}
		ProxyFactory factory = new ProxyFactory();
		for (Advice advice : getAdviceChain()) {
			factory.addAdvisor(new DefaultPointcutAdvisor(Pointcut.TRUE, advice));
		}
		factory.setProxyTargetClass(false);
		factory.addInterface(ContainerDelegate.class);
		factory.setTarget(this.delegate);
		this.proxy = (ContainerDelegate) factory.getProxy(ContainerDelegate.class.getClassLoader());
	}

	// -------------------------------------------------------------------------
	// Implementation of AbstractMessageListenerContainer's template methods
	// -------------------------------------------------------------------------

	/**
	 * Always use a shared Rabbit Connection.
	 * @return true
	 */
	protected final boolean sharedConnectionEnabled() {
		return true;
	}

	/**
	 * Creates the specified number of concurrent consumers, in the form of a Rabbit Channel plus associated
	 * MessageConsumer.
	 * @throws Exception Any Exception.
	 */
	@Override
	protected void doInitialize() throws Exception {
		checkMissingQueuesFatal();
		if (!this.isExposeListenerChannel() && this.transactionManager != null) {
			logger.warn("exposeListenerChannel=false is ignored when using a TransactionManager");
		}
		if (!this.taskExecutorSet && StringUtils.hasText(this.getBeanName())) {
			this.taskExecutor = new SimpleAsyncTaskExecutor(this.getBeanName() + "-");
			this.taskExecutorSet = true;
		}
		initializeProxy();
		if (this.transactionManager != null) {
			if (!isChannelTransacted()) {
				logger.debug("The 'channelTransacted' is coerced to 'true', when 'transactionManager' is provided");
				setChannelTransacted(true);
			}

		}
	}

	@ManagedMetric(metricType = MetricType.GAUGE)
	public int getActiveConsumerCount() {
		return this.cancellationLock.getCount();
	}

	/**
	 * Re-initializes this container's Rabbit message consumers, if not initialized already. Then submits each consumer
	 * to this container's task executor.
	 * @throws Exception Any Exception.
	 */
	@Override
	protected void doStart() throws Exception {
		if (getMessageListener() instanceof ListenerContainerAware) {
			Collection<String> expectedQueueNames = ((ListenerContainerAware) getMessageListener()).expectedQueueNames();
			if (expectedQueueNames != null) {
				String[] queueNames = getQueueNames();
				Assert.state(expectedQueueNames.size() == queueNames.length,
						"Listener expects us to be listening on '" + expectedQueueNames + "'; our queues: "
						+ Arrays.asList(queueNames));
				boolean found = false;
				for (String queueName : queueNames) {
					if (expectedQueueNames.contains(queueName)) {
						found = true;
					}
					else {
						found = false;
						break;
					}
				}
				Assert.state(found, "Listener expects us to be listening on '" + expectedQueueNames + "'; our queues: "
						+ Arrays.asList(queueNames));
			}
		}
		if (this.rabbitAdmin == null && this.getApplicationContext() != null) {
			Map<String, RabbitAdmin> admins = this.getApplicationContext().getBeansOfType(RabbitAdmin.class);
			if (admins.size() == 1) {
				this.rabbitAdmin = admins.values().iterator().next();
			}
			else {
				if (this.autoDeclare || this.mismatchedQueuesFatal) {
					if (logger.isDebugEnabled()) {
						logger.debug("For 'autoDeclare' and 'mismatchedQueuesFatal' to work, there must be exactly one "
								+ "RabbitAdmin in the context or you must inject one into this container; found: "
								+ admins.size() + " for container " + this.toString());
					}
				}
				if (this.mismatchedQueuesFatal) {
					throw new IllegalStateException("When 'mismatchedQueuesFatal' is 'true', there must be exactly "
							+ "one RabbitAdmin in the context or you must inject one into this container; found: "
							+ admins.size() + " for container " + this.toString());
				}
			}
		}
		checkMismatchedQueues();
		super.doStart();
		synchronized (this.consumersMonitor) {
			int newConsumers = initializeConsumers();
			if (this.consumers == null) {
				if (logger.isInfoEnabled()) {
					logger.info("Consumers were initialized and then cleared " +
							"(presumably the container was stopped concurrently)");
				}
				return;
			}
			if (newConsumers <= 0) {
				if (logger.isInfoEnabled()) {
					logger.info("Consumers are already running");
				}
				return;
			}
			Set<AsyncMessageProcessingConsumer> processors = new HashSet<AsyncMessageProcessingConsumer>();
			for (BlockingQueueConsumer consumer : this.consumers.keySet()) {
				AsyncMessageProcessingConsumer processor = new AsyncMessageProcessingConsumer(consumer);
				processors.add(processor);
				this.taskExecutor.execute(processor);
			}
			for (AsyncMessageProcessingConsumer processor : processors) {
				FatalListenerStartupException startupException = processor.getStartupException();
				if (startupException != null) {
					throw new AmqpIllegalStateException("Fatal exception on listener startup", startupException);
				}
			}
		}
	}

	@Override
	protected void doStop() {
		shutdown();
		super.doStop();
	}

	@Override
	protected void doShutdown() {

		if (!this.isRunning()) {
			return;
		}

		try {
			synchronized (this.consumersMonitor) {
				if (this.consumers != null) {
					for (BlockingQueueConsumer consumer : this.consumers.keySet()) {
						consumer.basicCancel();
					}
				}
			}
			logger.info("Waiting for workers to finish.");
			boolean finished = this.cancellationLock.await(this.shutdownTimeout, TimeUnit.MILLISECONDS);
			if (finished) {
				logger.info("Successfully waited for workers to finish.");
			}
			else {
				logger.info("Workers not finished.");
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.warn("Interrupted waiting for workers.  Continuing with shutdown.");
		}

		synchronized (this.consumersMonitor) {
			this.consumers = null;
		}

	}

	private boolean isActive(BlockingQueueConsumer consumer) {
		Boolean consumerActive;
		synchronized(this.consumersMonitor) {
			if (this.consumers != null) {
				Boolean active = this.consumers.get(consumer);
				consumerActive = active != null && active;
			}
			else {
				consumerActive = false;
			}
		}
		return consumerActive && this.isActive();
	}

	protected int initializeConsumers() {
		int count = 0;
		synchronized (this.consumersMonitor) {
			if (this.consumers == null) {
				this.cancellationLock.reset();
				this.consumers = new HashMap<BlockingQueueConsumer, Boolean>(this.concurrentConsumers);
				for (int i = 0; i < this.concurrentConsumers; i++) {
					BlockingQueueConsumer consumer = createBlockingQueueConsumer();
					this.consumers.put(consumer, true);
					count++;
				}
			}
		}
		return count;
	}

	private void checkMissingQueuesFatal() {
		if (!this.missingQueuesFatalSet) {
			try {
				ApplicationContext applicationContext = getApplicationContext();
				if (applicationContext != null) {
					Properties properties = applicationContext.getBean("spring.amqp.global.properties", Properties.class);
					String missingQueuesFatal = properties.getProperty("smlc.missing.queues.fatal");
					if (StringUtils.hasText(missingQueuesFatal)) {
						this.missingQueuesFatal = Boolean.parseBoolean(missingQueuesFatal);
					}
				}
			}
			catch (BeansException be) {
				if (logger.isDebugEnabled()) {
					logger.debug("No global properties bean");
				}
			}
		}
	}

	private void checkMismatchedQueues() {
		if (this.mismatchedQueuesFatal && this.rabbitAdmin != null) {
			try {
				this.rabbitAdmin.initialize();
			}
			catch (AmqpConnectException e) {
				logger.info("Broker not available; cannot check queue declarations");
			}
			catch (AmqpIOException e){
				if (RabbitUtils.isMismatchedQueueArgs(e)) {
					throw new FatalListenerStartupException("Mismatched queues", e);
				}
				else {
					logger.info("Failed to get connection during start(): " + e);
				}
			}
		}
	}

	protected void addAndStartConsumers(int delta) {
		synchronized (this.consumersMonitor) {
			if (this.consumers != null) {
				for (int i = 0; i < delta; i++) {
					BlockingQueueConsumer consumer = createBlockingQueueConsumer();
					this.consumers.put(consumer, true);
					AsyncMessageProcessingConsumer processor = new AsyncMessageProcessingConsumer(consumer);
					if (logger.isDebugEnabled()) {
						logger.debug("Starting a new consumer: " + consumer);
					}
					this.taskExecutor.execute(processor);
					try {
						FatalListenerStartupException startupException = processor.getStartupException();
						if (startupException != null) {
							this.consumers.remove(consumer);
							throw new AmqpIllegalStateException("Fatal exception on listener startup", startupException);
						}
					}
					catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
					}
					catch (Exception e) {
						consumer.stop();
						logger.error("Error starting new consumer", e);
						this.cancellationLock.release(consumer);
						this.consumers.remove(consumer);
					}
				}
			}
		}
	}

	private void considerAddingAConsumer() {
		synchronized(this.consumersMonitor) {
			if (this.consumers != null
					&& this.maxConcurrentConsumers != null && this.consumers.size() < this.maxConcurrentConsumers) {
				long now = System.currentTimeMillis();
				if (this.lastConsumerStarted + this.startConsumerMinInterval < now) {
					this.addAndStartConsumers(1);
					this.lastConsumerStarted = now;
				}
			}
		}
	}

	private void considerStoppingAConsumer(BlockingQueueConsumer consumer) {
		synchronized (this.consumersMonitor) {
			if (this.consumers != null && this.consumers.size() > this.concurrentConsumers) {
				long now = System.currentTimeMillis();
				if (this.lastConsumerStopped + this.stopConsumerMinInterval < now) {
					consumer.basicCancel();
					this.consumers.put(consumer, false);
					if (logger.isDebugEnabled()) {
						logger.debug("Idle consumer terminating: " + consumer);
					}
					this.lastConsumerStopped = now;
				}
			}
		}
	}

	private void queuesChanged() {
		synchronized (this.consumersMonitor) {
			if (this.consumers != null) {
				int count = 0;
				for (Entry<BlockingQueueConsumer, Boolean> consumer : this.consumers.entrySet()) {
					if (consumer.getValue()) {
						if (logger.isDebugEnabled()) {
							logger.debug("Queues changed; stopping consumer: " + consumer.getKey());
						}
						consumer.getKey().basicCancel();
						consumer.setValue(false);
						count++;
					}
				}
				this.addAndStartConsumers(count);
			}
		}
	}

	@Override
	protected boolean isChannelLocallyTransacted(Channel channel) {
		return super.isChannelLocallyTransacted(channel) && this.transactionManager == null;
	}

	protected BlockingQueueConsumer createBlockingQueueConsumer() {
		BlockingQueueConsumer consumer;
		String[] queues = getRequiredQueueNames();
		// There's no point prefetching less than the tx size, otherwise the consumer will stall because the broker
		// didn't get an ack for delivered messages
		int actualPrefetchCount = this.prefetchCount > this.txSize ? this.prefetchCount : this.txSize;
		consumer = new BlockingQueueConsumer(getConnectionFactory(), this.messagePropertiesConverter, this.cancellationLock,
				getAcknowledgeMode(), isChannelTransacted(), actualPrefetchCount, this.defaultRequeueRejected,
				this.consumerArgs, this.exclusive, queues);
		if (this.declarationRetries != null) {
			consumer.setDeclarationRetries(this.declarationRetries);
		}
		if (this.failedDeclarationRetryInterval != null) {
			consumer.setFailedDeclarationRetryInterval(this.failedDeclarationRetryInterval);
		}
		if (this.retryDeclarationInterval != null) {
			consumer.setRetryDeclarationInterval(this.retryDeclarationInterval);
		}
		if (this.consumerTagStrategy != null) {
			consumer.setTagStrategy(this.consumerTagStrategy);
		}
		consumer.setBackOffExecution(this.recoveryBackOff.start());
		return consumer;
	}

	private void restart(BlockingQueueConsumer consumer) {
		synchronized (this.consumersMonitor) {
			if (this.consumers != null) {
				try {
					// Need to recycle the channel in this consumer
					consumer.stop();
					// Ensure consumer counts are correct (another is going
					// to start because of the exception, but
					// we haven't counted down yet)
					this.cancellationLock.release(consumer);
					this.consumers.remove(consumer);
					BlockingQueueConsumer newConsumer = createBlockingQueueConsumer();
					newConsumer.setBackOffExecution(consumer.getBackOffExecution());
					consumer = newConsumer;
					this.consumers.put(consumer, true);
				}
				catch (RuntimeException e) {
					logger.warn("Consumer failed irretrievably on restart. " + e.getClass() + ": " + e.getMessage());
					// Re-throw and have it logged properly by the caller.
					throw e;
				}
				this.taskExecutor.execute(new AsyncMessageProcessingConsumer(consumer));
			}
		}
	}

	/**
	 * Use {@link RabbitAdmin#initialize()} to redeclare everything if necessary.
	 * Since auto deletion of a queue can cause upstream elements
	 * (bindings, exchanges) to be deleted too, everything needs to be redeclared if
	 * a queue is missing.
	 * Declaration is idempotent so, aside from some network chatter, there is no issue,
	 * and we only will do it if we detect our queue is gone.
	 * <p>
	 * In general it makes sense only for the 'auto-delete' or 'expired' queues,
	 * but with the server TTL policy we don't have ability to determine 'expiration'
	 * option for the queue.
	 * <p>
	 * Starting with version 1.6, if
	 * {@link #setMismatchedQueuesFatal(boolean) mismatchedQueuesFatal} is true,
	 * the declarations are always attempted during restart so the listener will
	 * fail with a fatal error if mismatches occur.
	 */
	private synchronized void redeclareElementsIfNecessary() {
		if (this.rabbitAdmin == null) {
			return;
		}
		try {
			ApplicationContext applicationContext = this.getApplicationContext();
			if (applicationContext != null) {
				Set<String> queueNames = this.getQueueNamesAsSet();
				Map<String, Queue> queueBeans = applicationContext.getBeansOfType(Queue.class);
				for (Entry<String, Queue> entry : queueBeans.entrySet()) {
					Queue queue = entry.getValue();
					if (this.mismatchedQueuesFatal || (queueNames.contains(queue.getName()) &&
							this.rabbitAdmin.getQueueProperties(queue.getName()) == null)) {
						if (logger.isDebugEnabled()) {
							logger.debug("Redeclaring context exchanges, queues, bindings.");
						}
						this.rabbitAdmin.initialize();
						return;
					}
				}
			}
		}
		catch (Exception e) {
			if (RabbitUtils.isMismatchedQueueArgs(e)) {
				throw new FatalListenerStartupException("Mismatched queues", e);
			}
			logger.error("Failed to check/redeclare auto-delete queue(s).", e);
		}
	}

	private boolean receiveAndExecute(final BlockingQueueConsumer consumer) throws Throwable {

		if (this.transactionManager != null) {
			try {
				return new TransactionTemplate(this.transactionManager, this.transactionAttribute)
						.execute(new TransactionCallback<Boolean>() {
							@Override
							public Boolean doInTransaction(TransactionStatus status) {
								ConnectionFactoryUtils.bindResourceToTransaction(
										new RabbitResourceHolder(consumer.getChannel(), false),
										getConnectionFactory(), true);
								try {
									return doReceiveAndExecute(consumer);
								}
								catch (RuntimeException e) {
									throw e;
								}
								catch (Throwable e) {//NOSONAR
									// ok to catch Throwable here because we re-throw it below
									throw new WrappedTransactionException(e);
								}
							}
						});
			}
			catch (WrappedTransactionException e) {
				throw e.getCause();
			}
		}

		return doReceiveAndExecute(consumer);

	}

	private boolean doReceiveAndExecute(BlockingQueueConsumer consumer) throws Throwable {

		Channel channel = consumer.getChannel();

		for (int i = 0; i < this.txSize; i++) {

			logger.trace("Waiting for message from consumer.");
			Message message = consumer.nextMessage(this.receiveTimeout);
			if (message == null) {
				break;
			}
			try {
				executeListener(channel, message);
			}
			catch (ImmediateAcknowledgeAmqpException e) {
				break;
			}
			catch (Throwable ex) {//NOSONAR
				consumer.rollbackOnExceptionIfNecessary(ex);
				throw ex;
			}

		}

		return consumer.commitIfNecessary(isChannelLocallyTransacted(channel));

	}

	private Advice[] getAdviceChain() {
		return this.adviceChain;
	}

	@Override
	public String toString() {
		return "SimpleMessageListenerContainer "
				+ (getBeanName() != null ? "(" + getBeanName() + ") " : "")
				+ "[concurrentConsumers=" + this.concurrentConsumers
				+ (this.maxConcurrentConsumers != null ? ", maxConcurrentConsumers=" + this.maxConcurrentConsumers : "")
				+ ", queueNames=" + Arrays.toString(getQueueNames()) + "]";
	}

	private class AsyncMessageProcessingConsumer implements Runnable {

		private final BlockingQueueConsumer consumer;

		private final CountDownLatch start;

		private volatile FatalListenerStartupException startupException;

		private AsyncMessageProcessingConsumer(BlockingQueueConsumer consumer) {
			this.consumer = consumer;
			this.start = new CountDownLatch(1);
		}

		/**
		 * Retrieve the fatal startup exception if this processor completely failed to locate the broker resources it
		 * needed. Blocks up to 60 seconds waiting for an exception to occur
		 * (but should always return promptly in normal circumstances).
		 * No longer fatal if the processor does not start up in 60 seconds.
		 * @return a startup exception if there was one
		 * @throws TimeoutException if the consumer hasn't started
		 * @throws InterruptedException if the consumer startup is interrupted
		 */
		private FatalListenerStartupException getStartupException() throws TimeoutException, InterruptedException {
			this.start.await(60000L, TimeUnit.MILLISECONDS);//NOSONAR - ignore return value
			return this.startupException;
		}

		@Override
		public void run() {

			boolean aborted = false;

			int consecutiveIdles = 0;

			int consecutiveMessages = 0;

			try {

				try {
					if (SimpleMessageListenerContainer.this.autoDeclare) {
						SimpleMessageListenerContainer.this.redeclareElementsIfNecessary();
					}
					this.consumer.start();
					this.start.countDown();
				}
				catch (QueuesNotAvailableException e) {
					if (SimpleMessageListenerContainer.this.missingQueuesFatal) {
						throw e;
					}
					else {
						this.start.countDown();
						handleStartupFailure(this.consumer.getBackOffExecution());
						throw e;
					}
				}
				catch (FatalListenerStartupException ex) {
					throw ex;
				}
				catch (Throwable t) {//NOSONAR
					this.start.countDown();
					handleStartupFailure(this.consumer.getBackOffExecution());
					throw t;
				}

				if (SimpleMessageListenerContainer.this.transactionManager != null) {
					/*
					 * Register the consumer's channel so it will be used by the transaction manager
					 * if it's an instance of RabbitTransactionManager.
					 */
					ConsumerChannelRegistry.registerConsumerChannel(this.consumer.getChannel(), getConnectionFactory());
				}

				while (isActive(this.consumer) || this.consumer.hasDelivery()) {
					try {
						boolean receivedOk = receiveAndExecute(this.consumer); // At least one message received
						if (SimpleMessageListenerContainer.this.maxConcurrentConsumers != null) {
							if (receivedOk) {
								if (isActive(this.consumer)) {
									consecutiveIdles = 0;
									if (consecutiveMessages++ > SimpleMessageListenerContainer.this.consecutiveActiveTrigger) {
										considerAddingAConsumer();
										consecutiveMessages = 0;
									}
								}
							}
							else {
								consecutiveMessages = 0;
								if (consecutiveIdles++ > SimpleMessageListenerContainer.this.consecutiveIdleTrigger) {
									considerStoppingAConsumer(this.consumer);
									consecutiveIdles = 0;
								}
							}
						}
						if (SimpleMessageListenerContainer.this.idleEventInterval != null) {
							if (receivedOk) {
								SimpleMessageListenerContainer.this.lastReceive = System.currentTimeMillis();
							}
							else {
								long now = System.currentTimeMillis();
								long lastAlertAt = SimpleMessageListenerContainer.this.lastNoMessageAlert.get();
								long lastReceive = SimpleMessageListenerContainer.this.lastReceive;
								if (now > lastReceive + SimpleMessageListenerContainer.this.idleEventInterval
										&& now > lastAlertAt + SimpleMessageListenerContainer.this.idleEventInterval
										&& SimpleMessageListenerContainer.this.lastNoMessageAlert
												.compareAndSet(lastAlertAt, now)) {
									publishIdleContainerEvent(now - lastReceive);
								}
							}
						}
					}
					catch (ListenerExecutionFailedException ex) {
						// Continue to process, otherwise re-throw
						if (ex.getCause() instanceof NoSuchMethodException) {
							throw new FatalListenerExecutionException("Invalid listener", ex);
						}
					}
					catch (AmqpRejectAndDontRequeueException rejectEx) {
						/*
						 *  These will normally be wrapped by an LEFE if thrown by the
						 *  listener, but we will also honor it if thrown by an
						 *  error handler.
						 */
					}
				}

			}
			catch (InterruptedException e) {
				logger.debug("Consumer thread interrupted, processing stopped.");
				Thread.currentThread().interrupt();
				aborted = true;
				publishConsumerFailedEvent("Consumer thread interrupted, processing stopped", true, e);
			}
			catch (QueuesNotAvailableException ex) {
				if (SimpleMessageListenerContainer.this.missingQueuesFatal) {
					logger.error("Consumer received fatal exception on startup", ex);
					this.startupException = ex;
					// Fatal, but no point re-throwing, so just abort.
					aborted = true;
				}
				publishConsumerFailedEvent("Consumer queue(s) not available", aborted, ex);
			}
			catch (FatalListenerStartupException ex) {
				logger.error("Consumer received fatal exception on startup", ex);
				this.startupException = ex;
				// Fatal, but no point re-throwing, so just abort.
				aborted = true;
				publishConsumerFailedEvent("Consumer received fatal exception on startup", true, ex);
			}
			catch (FatalListenerExecutionException ex) {
				logger.error("Consumer received fatal exception during processing", ex);
				// Fatal, but no point re-throwing, so just abort.
				aborted = true;
				publishConsumerFailedEvent("Consumer received fatal exception during processing", true, ex);
			}
			catch (ShutdownSignalException e) {
				if (RabbitUtils.isNormalShutdown(e)) {
					if (logger.isDebugEnabled()) {
						logger.debug("Consumer received Shutdown Signal, processing stopped: " + e.getMessage());
					}
				}
				else {
					this.logConsumerException(e);
				}
			}
			catch (AmqpIOException e) {
				if (e.getCause() instanceof IOException && e.getCause().getCause() instanceof ShutdownSignalException
						&& e.getCause().getCause().getMessage().contains("in exclusive use")) {
					SimpleMessageListenerContainer.this.exclusiveConsumerExceptionLogger.log(logger,
							"Exclusive consumer failure", e.getCause().getCause());
					publishConsumerFailedEvent("Consumer raised exception, attempting restart", false, e);
				}
				else {
					this.logConsumerException(e);
				}
			}
			catch (Error e) {//NOSONAR
				// ok to catch Error - we're aborting so will stop
				logger.error("Consumer thread error, thread abort.", e);
				aborted = true;
			}
			catch (Throwable t) {//NOSONAR
				// by now, it must be an exception
				if (isActive()) {
					this.logConsumerException(t);
				}
			}
			finally {
				if (SimpleMessageListenerContainer.this.transactionManager != null) {
					ConsumerChannelRegistry.unRegisterConsumerChannel();
				}
			}

			// In all cases count down to allow container to progress beyond startup
			this.start.countDown();

			if (!isActive(this.consumer) || aborted) {
				logger.debug("Cancelling " + this.consumer);
				try {
					this.consumer.stop();
					SimpleMessageListenerContainer.this.cancellationLock.release(this.consumer);
					synchronized (SimpleMessageListenerContainer.this.consumersMonitor) {
						if (SimpleMessageListenerContainer.this.consumers != null) {
							SimpleMessageListenerContainer.this.consumers.remove(this.consumer);
						}
					}
				}
				catch (AmqpException e) {
					logger.info("Could not cancel message consumer", e);
				}
				if (aborted) {
					logger.error("Stopping container from aborted consumer");
					stop();
				}
			}
			else {
				logger.info("Restarting " + this.consumer);
				restart(this.consumer);
			}

		}

		private void logConsumerException(Throwable t) {
			if (logger.isDebugEnabled()
					|| !(t instanceof AmqpConnectException  || t instanceof ConsumerCancelledException)) {
				logger.warn(
						"Consumer raised exception, processing can restart if the connection factory supports it",
						t);
			}
			else {
				logger.warn("Consumer raised exception, processing can restart if the connection factory supports it. "
						+ "Exception summary: " + t);
			}
			publishConsumerFailedEvent("Consumer raised exception, attempting restart", false, t);
		}

		private void publishConsumerFailedEvent(String reason, boolean fatal, Throwable t) {
			if (SimpleMessageListenerContainer.this.applicationEventPublisher != null) {
				SimpleMessageListenerContainer.this.applicationEventPublisher
						.publishEvent(new ListenerContainerConsumerFailedEvent(SimpleMessageListenerContainer.this,
								reason, t, fatal));
			}
		}

		private void publishIdleContainerEvent(long idleTime) {
			if (SimpleMessageListenerContainer.this.applicationEventPublisher != null) {
				SimpleMessageListenerContainer.this.applicationEventPublisher.publishEvent(
						new ListenerContainerIdleEvent(SimpleMessageListenerContainer.this, idleTime, getListenerId(),
								getQueueNames()));
			}
		}

	}

	@Override
	protected void invokeListener(Channel channel, Message message) throws Exception {
		this.proxy.invokeListener(channel, message);
	}

	/**
	 * Wait for a period determined by the {@link #setRecoveryInterval(long) recoveryInterval}
	 * or {@link #setRecoveryBackOff(BackOff)} to give the container a
	 * chance to recover from consumer startup failure, e.g. if the broker is down.
	 * @param backOffExecution the BackOffExecution to get the {@code recoveryInterval}
	 * @throws Exception if the shared connection still can't be established
	 */
	protected void handleStartupFailure(BackOffExecution backOffExecution) throws Exception {
		long recoveryInterval = backOffExecution.nextBackOff();
		if (BackOffExecution.STOP == recoveryInterval) {
			synchronized (this) {
				if (isActive()) {
					logger.warn("stopping container - restart recovery attempts exhausted");
					stop();
				}
			}
			return;
		}
		try {
			if (logger.isDebugEnabled() && isActive()) {
				logger.debug("Recovering consumer in " + recoveryInterval + " ms.");
			}
			long timeout = System.currentTimeMillis() + recoveryInterval;
			while (isActive() && System.currentTimeMillis() < timeout) {
				Thread.sleep(200);
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Unrecoverable interruption on consumer restart");
		}
	}

	@SuppressWarnings("serial")
	private static class WrappedTransactionException extends RuntimeException {

		private WrappedTransactionException(Throwable cause) {
			super(cause);
		}

	}

	/**
	 * Default implementation of {@link ConditionalExceptionLogger} for logging exclusive
	 * consumer failures.
	 * @since 1.5
	 */
	private static class DefaultExclusiveConsumerLogger implements ConditionalExceptionLogger {

		@Override
		public void log(Log logger, String message, Throwable t) {
			if (t instanceof ShutdownSignalException) {
				ShutdownSignalException cause = (ShutdownSignalException) t;
				if (RabbitUtils.isExclusiveUseChannelClose(cause)) {
					if (logger.isWarnEnabled()) {
						logger.warn(message + ": " + cause.toString());
					}
				}
				else if (!RabbitUtils.isNormalChannelClose(cause)) {
					logger.error(message + ": " + cause.getMessage());
				}
			}
			else {
				logger.error("Unexpected invocation of " + this.getClass() + ", with message: " + message, t);
			}
		}

	}

}
