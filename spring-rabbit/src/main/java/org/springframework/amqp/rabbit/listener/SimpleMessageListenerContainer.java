/*
 * Copyright 2002-2017 the original author or authors.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.ConsumerChannelRegistry;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.connection.SimpleResourceHolder;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerExecutionException;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.support.ConsumerCancelledException;
import org.springframework.amqp.rabbit.support.ListenerContainerAware;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.support.MetricType;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 * @author Alex Panchenko
 *
 * @since 1.0
 */
public class SimpleMessageListenerContainer extends AbstractMessageListenerContainer {

	private static final long DEFAULT_START_CONSUMER_MIN_INTERVAL = 10000;

	private static final long DEFAULT_STOP_CONSUMER_MIN_INTERVAL = 60000;

	private static final long DEFAULT_CONSUMER_START_TIMEOUT = 60000L;

	private static final int DEFAULT_CONSECUTIVE_ACTIVE_TRIGGER = 10;

	private static final int DEFAULT_CONSECUTIVE_IDLE_TRIGGER = 10;

	public static final long DEFAULT_RECEIVE_TIMEOUT = 1000;

	private final AtomicLong lastNoMessageAlert = new AtomicLong();

	private final AtomicReference<Thread> containerStoppingForAbort = new AtomicReference<>();

	private final BlockingQueue<ListenerContainerConsumerFailedEvent> abortEvents = new LinkedBlockingQueue<>();

	private volatile long startConsumerMinInterval = DEFAULT_START_CONSUMER_MIN_INTERVAL;

	private volatile long stopConsumerMinInterval = DEFAULT_STOP_CONSUMER_MIN_INTERVAL;

	private volatile int consecutiveActiveTrigger = DEFAULT_CONSECUTIVE_ACTIVE_TRIGGER;

	private volatile int consecutiveIdleTrigger = DEFAULT_CONSECUTIVE_IDLE_TRIGGER;

	private volatile int txSize = 1;

	private volatile int concurrentConsumers = 1;

	private volatile Integer maxConcurrentConsumers;

	private volatile long lastConsumerStarted;

	private volatile long lastConsumerStopped;

	private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

	private Set<BlockingQueueConsumer> consumers;

	private final ActiveObjectCounter<BlockingQueueConsumer> cancellationLock = new ActiveObjectCounter<>();

	private Integer declarationRetries;

	private Long retryDeclarationInterval;

	private TransactionTemplate transactionTemplate;

	private long consumerStartTimeout = DEFAULT_CONSUMER_START_TIMEOUT;

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
		setConnectionFactory(connectionFactory);
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
		Assert.isTrue(!isExclusive() || concurrentConsumers == 1,
				"When the consumer is exclusive, the concurrency must be 1");
		if (this.maxConcurrentConsumers != null) {
			Assert.isTrue(concurrentConsumers <= this.maxConcurrentConsumers,
					"'concurrentConsumers' cannot be more than 'maxConcurrentConsumers'");
		}
		synchronized (this.consumersMonitor) {
			if (logger.isDebugEnabled()) {
				logger.debug("Changing consumers from " + this.concurrentConsumers + " to " + concurrentConsumers);
			}
			int delta = this.concurrentConsumers - concurrentConsumers;
			this.concurrentConsumers = concurrentConsumers;
			if (isActive() && this.consumers != null) {
				if (delta > 0) {
					Iterator<BlockingQueueConsumer> consumerIterator = this.consumers.iterator();
					while (consumerIterator.hasNext() && delta > 0) {
						BlockingQueueConsumer consumer = consumerIterator.next();
						consumer.basicCancel(true);
						consumerIterator.remove();
						delta--;
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
		Assert.isTrue(!isExclusive() || maxConcurrentConsumers == 1,
				"When the consumer is exclusive, the concurrency must be 1");
		this.maxConcurrentConsumers = maxConcurrentConsumers;
	}

	/**
	 * Specify concurrency limits via a "lower-upper" String, e.g. "5-10", or a simple
	 * upper limit String, e.g. "10" (a fixed number of consumers).
	 * <p>This listener container will always hold on to the minimum number of consumers
	 * ({@link #setConcurrentConsumers}) and will slowly scale up to the maximum number
	 * of consumers {@link #setMaxConcurrentConsumers} in case of increasing load.
	 * @param concurrency the concurrency.
	 * @since 2.0
	 */
	public void setConcurrency(String concurrency) {
		try {
			int separatorIndex = concurrency.indexOf('-');
			if (separatorIndex != -1) {
				setConcurrentConsumers(Integer.parseInt(concurrency.substring(0, separatorIndex)));
				setMaxConcurrentConsumers(
						Integer.parseInt(concurrency.substring(separatorIndex + 1, concurrency.length())));
			}
			else {
				setConcurrentConsumers(Integer.parseInt(concurrency));
			}
		}
		catch (NumberFormatException ex) {
			throw new IllegalArgumentException("Invalid concurrency value [" + concurrency + "]: only " +
					"single fixed integer (e.g. \"5\") and minimum-maximum combo (e.g. \"3-5\") supported.");
		}
	}

	/**
	 * Set to true for an exclusive consumer - if true, the concurrency must be 1.
	 * @param exclusive true for an exclusive consumer.
	 */
	@Override
	public final void setExclusive(boolean exclusive) {
		Assert.isTrue(!exclusive || (this.concurrentConsumers == 1
						&& (this.maxConcurrentConsumers == null || this.maxConcurrentConsumers == 1)),
				"When the consumer is exclusive, the concurrency must be 1");
		super.setExclusive(exclusive);
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
	 * Tells the container how many messages to process in a single transaction (if the channel is transactional). For
	 * best results it should be less than or equal to {@link #setPrefetchCount(int) the prefetch count}. Also affects
	 * how often acks are sent when using {@link AcknowledgeMode#AUTO} - one ack per txSize. Default is 1.
	 * @param txSize the transaction size
	 */
	public void setTxSize(int txSize) {
		Assert.isTrue(txSize > 0, "'txSize' must be > 0");
		this.txSize = txSize;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * When true, if the queues are removed while the container is running, the container
	 * is stopped.
	 * <p>
	 * Defaults to true for this container.
	 */
	@Override
	public void setMissingQueuesFatal(boolean missingQueuesFatal) {
		super.setMissingQueuesFatal(missingQueuesFatal);
	}

	@Override
	public void setQueueNames(String... queueName) {
		super.setQueueNames(queueName);
		this.queuesChanged();
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
	 * Set the number of retries after passive queue declaration fails.
	 * @param declarationRetries The number of retries, default 3.
	 * @since 1.3.9
	 * @see #setFailedDeclarationRetryInterval(long)
	 */
	public void setDeclarationRetries(int declarationRetries) {
		this.declarationRetries = declarationRetries;
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
	 * When starting a consumer, if this time (ms) elapses before the consumer starts, an
	 * error log is written; one possible cause would be if the
	 * {@link #setTaskExecutor(java.util.concurrent.Executor) taskExecutor} has
	 * insufficient threads to support the container concurrency. Default 60000.
	 * @param consumerStartTimeout the timeout.
	 * @since 1.7.5
	 */
	public void setConsumerStartTimeout(long consumerStartTimeout) {
		this.consumerStartTimeout = consumerStartTimeout;
	}

	/**
	 * Avoid the possibility of not configuring the CachingConnectionFactory in sync with the number of concurrent
	 * consumers.
	 */
	@Override
	protected void validateConfiguration() {

		super.validateConfiguration();

		Assert.state(
				!(getAcknowledgeMode().isAutoAck() && getTransactionManager() != null),
				"The acknowledgeMode is NONE (autoack in Rabbit terms) which is not consistent with having an "
						+ "external transaction manager. Either use a different AcknowledgeMode or make sure " +
						"the transactionManager is null.");

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

	@Override
	protected void doInitialize() throws Exception {

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
				Assert.state(found, () -> "Listener expects us to be listening on '" + expectedQueueNames + "'; our queues: "
						+ Arrays.asList(queueNames));
			}
		}
		super.doStart();
		synchronized (this.consumersMonitor) {
			if (this.consumers != null) {
				throw new IllegalStateException("A stopped container should not have consumers");
			}
			int newConsumers = initializeConsumers();
			if (this.consumers == null) {
				logger.info("Consumers were initialized and then cleared " +
						"(presumably the container was stopped concurrently)");
				return;
			}
			if (newConsumers <= 0) {
				if (logger.isInfoEnabled()) {
					logger.info("Consumers are already running");
				}
				return;
			}
			Set<AsyncMessageProcessingConsumer> processors = new HashSet<AsyncMessageProcessingConsumer>();
			for (BlockingQueueConsumer consumer : this.consumers) {
				AsyncMessageProcessingConsumer processor = new AsyncMessageProcessingConsumer(consumer);
				processors.add(processor);
				getTaskExecutor().execute(processor);
				if (getApplicationEventPublisher() != null) {
					getApplicationEventPublisher().publishEvent(new AsyncConsumerStartedEvent(this, consumer));
				}
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
	protected void doShutdown() {
		Thread thread = this.containerStoppingForAbort.get();
		if (thread != null && !thread.equals(Thread.currentThread())) {
			logger.info("Shutdown ignored - container is stopping due to an aborted consumer");
			return;
		}

		try {
			List<BlockingQueueConsumer> canceledConsumers = new ArrayList<>();
			synchronized (this.consumersMonitor) {
				if (this.consumers != null) {
					Iterator<BlockingQueueConsumer> consumerIterator = this.consumers.iterator();
					while (consumerIterator.hasNext()) {
						BlockingQueueConsumer consumer = consumerIterator.next();
						consumer.basicCancel(true);
						canceledConsumers.add(consumer);
						consumerIterator.remove();
						if (consumer.declaring) {
							consumer.thread.interrupt();
						}
					}
				}
				else {
					logger.info("Shutdown ignored - container is already stopped");
					return;
				}
			}
			logger.info("Waiting for workers to finish.");
			boolean finished = this.cancellationLock.await(getShutdownTimeout(), TimeUnit.MILLISECONDS);
			if (finished) {
				logger.info("Successfully waited for workers to finish.");
			}
			else {
				logger.info("Workers not finished.");
				if (isForceCloseChannel()) {
					canceledConsumers.forEach(consumer -> {
						if (logger.isWarnEnabled()) {
							logger.warn("Closing channel for unresponsive consumer: " + consumer);
						}
						consumer.stop();
					});
				}
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.warn("Interrupted waiting for workers.  Continuing with shutdown.");
		}

		synchronized (this.consumersMonitor) {
			this.consumers = null;
			this.cancellationLock.deactivate();
		}

	}

	private boolean isActive(BlockingQueueConsumer consumer) {
		boolean consumerActive;
		synchronized (this.consumersMonitor) {
			consumerActive = this.consumers != null && this.consumers.contains(consumer);
		}
		return consumerActive && this.isActive();
	}

	protected int initializeConsumers() {
		int count = 0;
		synchronized (this.consumersMonitor) {
			if (this.consumers == null) {
				this.cancellationLock.reset();
				this.consumers = new HashSet<BlockingQueueConsumer>(this.concurrentConsumers);
				for (int i = 0; i < this.concurrentConsumers; i++) {
					BlockingQueueConsumer consumer = createBlockingQueueConsumer();
					this.consumers.add(consumer);
					count++;
				}
			}
		}
		return count;
	}

	protected void addAndStartConsumers(int delta) {
		synchronized (this.consumersMonitor) {
			if (this.consumers != null) {
				for (int i = 0; i < delta; i++) {
					BlockingQueueConsumer consumer = createBlockingQueueConsumer();
					this.consumers.add(consumer);
					AsyncMessageProcessingConsumer processor = new AsyncMessageProcessingConsumer(consumer);
					if (logger.isDebugEnabled()) {
						logger.debug("Starting a new consumer: " + consumer);
					}
					getTaskExecutor().execute(processor);
					if (this.getApplicationEventPublisher() != null) {
						this.getApplicationEventPublisher().publishEvent(new AsyncConsumerStartedEvent(this, consumer));
					}
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
		synchronized (this.consumersMonitor) {
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
					consumer.basicCancel(true);
					this.consumers.remove(consumer);
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
				Iterator<BlockingQueueConsumer> consumerIterator = this.consumers.iterator();
				while (consumerIterator.hasNext()) {
					BlockingQueueConsumer consumer = consumerIterator.next();
					if (logger.isDebugEnabled()) {
						logger.debug("Queues changed; stopping consumer: " + consumer);
					}
					consumer.basicCancel(true);
					consumerIterator.remove();
					count++;
				}
				this.addAndStartConsumers(count);
			}
		}
	}

	protected BlockingQueueConsumer createBlockingQueueConsumer() {
		BlockingQueueConsumer consumer;
		String[] queues = getQueueNames();
		// There's no point prefetching less than the tx size, otherwise the consumer will stall because the broker
		// didn't get an ack for delivered messages
		int actualPrefetchCount = getPrefetchCount() > this.txSize ? getPrefetchCount() : this.txSize;
		consumer = new BlockingQueueConsumer(getConnectionFactory(), getMessagePropertiesConverter(),
				this.cancellationLock, getAcknowledgeMode(), isChannelTransacted(), actualPrefetchCount,
				isDefaultRequeueRejected(), getConsumerArguments(), isNoLocal(), isExclusive(), queues);
		if (this.declarationRetries != null) {
			consumer.setDeclarationRetries(this.declarationRetries);
		}
		if (getFailedDeclarationRetryInterval() > 0) {
			consumer.setFailedDeclarationRetryInterval(getFailedDeclarationRetryInterval());
		}
		if (this.retryDeclarationInterval != null) {
			consumer.setRetryDeclarationInterval(this.retryDeclarationInterval);
		}
		if (getConsumerTagStrategy() != null) {
			consumer.setTagStrategy(getConsumerTagStrategy());
		}
		consumer.setBackOffExecution(getRecoveryBackOff().start());
		consumer.setShutdownTimeout(getShutdownTimeout());
		consumer.setApplicationEventPublisher(getApplicationEventPublisher());
		return consumer;
	}

	private void restart(BlockingQueueConsumer oldConsumer) {
		BlockingQueueConsumer consumer = oldConsumer;
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
					if (!isActive()) {
						// Do not restart - container is stopping
						return;
					}
					BlockingQueueConsumer newConsumer = createBlockingQueueConsumer();
					newConsumer.setBackOffExecution(consumer.getBackOffExecution());
					consumer = newConsumer;
					this.consumers.add(consumer);
					if (getApplicationEventPublisher() != null) {
						getApplicationEventPublisher()
								.publishEvent(new AsyncConsumerRestartedEvent(this, oldConsumer, newConsumer));
					}
				}
				catch (RuntimeException e) {
					logger.warn("Consumer failed irretrievably on restart. " + e.getClass() + ": " + e.getMessage());
					// Re-throw and have it logged properly by the caller.
					throw e;
				}
				getTaskExecutor()
						.execute(new AsyncMessageProcessingConsumer(consumer));
			}
		}
	}

	private boolean receiveAndExecute(final BlockingQueueConsumer consumer) throws Throwable {

		if (getTransactionManager() != null) {
			try {
				if (this.transactionTemplate == null) {
					this.transactionTemplate =
							new TransactionTemplate(getTransactionManager(), getTransactionAttribute());
				}
				return this.transactionTemplate
						.execute(status -> {
							RabbitResourceHolder resourceHolder = ConnectionFactoryUtils.bindResourceToTransaction(
									new RabbitResourceHolder(consumer.getChannel(), false),
									getConnectionFactory(), true);
							// unbound in ResourceHolderSynchronization.beforeCompletion()
							try {
								return doReceiveAndExecute(consumer);
							}
							catch (RuntimeException e1) {
								prepareHolderForRollback(resourceHolder, e1);
								throw e1;
							}
							catch (Throwable e2) { //NOSONAR
								// ok to catch Throwable here because we re-throw it below
								throw new WrappedTransactionException(e2);
							}
						});
			}
			catch (WrappedTransactionException e) {
				throw e.getCause();
			}
		}

		return doReceiveAndExecute(consumer);

	}

	private boolean doReceiveAndExecute(BlockingQueueConsumer consumer) throws Throwable { //NOSONAR

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
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("User requested ack for failed delivery '"
							+ e.getMessage() + "': "
							+ message.getMessageProperties().getDeliveryTag());
				}
				break;
			}
			catch (Throwable ex) { //NOSONAR
				if (causeChainHasImmediateAcknowledgeAmqpException(ex)) {
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("User requested ack for failed delivery: "
								+ message.getMessageProperties().getDeliveryTag());
					}
					break;
				}
				if (getTransactionManager() != null) {
					if (getTransactionAttribute().rollbackOn(ex)) {
						RabbitResourceHolder resourceHolder = (RabbitResourceHolder) TransactionSynchronizationManager
								.getResource(getConnectionFactory());
						if (resourceHolder != null) {
							consumer.clearDeliveryTags();
						}
						else {
							/*
							 * If we don't actually have a transaction, we have to roll back
							 * manually. See prepareHolderForRollback().
							 */
							consumer.rollbackOnExceptionIfNecessary(ex);
						}
						throw ex; // encompassing transaction will handle the rollback.
					}
					else {
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("No rollback for " + ex);
						}
						break;
					}
				}
				else {
					consumer.rollbackOnExceptionIfNecessary(ex);
					throw ex;
				}
			}
		}

		return consumer.commitIfNecessary(isChannelLocallyTransacted());

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

	@Override
	protected void publishConsumerFailedEvent(String reason, boolean fatal, Throwable t) {
		if (!fatal || !isRunning()) {
			super.publishConsumerFailedEvent(reason, fatal, t);
		}
		else {
			try {
				this.abortEvents.put(new ListenerContainerConsumerFailedEvent(this, reason, t, fatal));
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public String toString() {
		return "SimpleMessageListenerContainer "
				+ (getBeanName() != null ? "(" + getBeanName() + ") " : "")
				+ "[concurrentConsumers=" + this.concurrentConsumers
				+ (this.maxConcurrentConsumers != null ? ", maxConcurrentConsumers=" + this.maxConcurrentConsumers : "")
				+ ", queueNames=" + Arrays.toString(getQueueNames()) + "]";
	}

	private final class AsyncMessageProcessingConsumer implements Runnable {

		private final BlockingQueueConsumer consumer;

		private final CountDownLatch start;

		private volatile FatalListenerStartupException startupException;

		AsyncMessageProcessingConsumer(BlockingQueueConsumer consumer) {
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
		private FatalListenerStartupException getStartupException() throws TimeoutException,
				InterruptedException {
			if (!this.start.await(
					SimpleMessageListenerContainer.this.consumerStartTimeout, TimeUnit.MILLISECONDS)) {
				logger.error("Consumer failed to start in "
						+ SimpleMessageListenerContainer.this.consumerStartTimeout
						+ " milliseconds; does the task executor have enough threads to support the container "
						+ "concurrency?");
			}
			return this.startupException;
		}

		@Override
		public void run() {
			if (!isActive()) {
				return;
			}

			boolean aborted = false;

			int consecutiveIdles = 0;

			int consecutiveMessages = 0;

			this.consumer.setLocallyTransacted(isChannelLocallyTransacted());

			String routingLookupKey = getRoutingLookupKey();
			if (routingLookupKey != null) {
				SimpleResourceHolder.bind(getRoutingConnectionFactory(), routingLookupKey);
			}

			if (this.consumer.getQueueCount() < 1) {
				if (logger.isDebugEnabled()) {
					logger.debug("Consumer stopping; no queues for " + this.consumer);
				}
				SimpleMessageListenerContainer.this.cancellationLock.release(this.consumer);
				if (getApplicationEventPublisher() != null) {
					getApplicationEventPublisher().publishEvent(
							new AsyncConsumerStoppedEvent(SimpleMessageListenerContainer.this, this.consumer));
				}
				this.start.countDown();
				return;
			}

			try {

				try {
					redeclareElementsIfNecessary();
					this.consumer.start();
					this.start.countDown();
				}
				catch (QueuesNotAvailableException e) {
					if (isMissingQueuesFatal()) {
						throw e;
					}
					else {
						this.start.countDown();
						handleStartupFailure(this.consumer.getBackOffExecution());
						throw e;
					}
				}
				catch (FatalListenerStartupException ex) {
					if (isPossibleAuthenticationFailureFatal()) {
						throw ex;
					}
					else {
						Throwable possibleAuthException = ex.getCause().getCause();
						if (possibleAuthException == null ||
								!(possibleAuthException instanceof PossibleAuthenticationFailureException)) {
							throw ex;
						}
						else {
							this.start.countDown();
							handleStartupFailure(this.consumer.getBackOffExecution());
							throw possibleAuthException;
						}
					}
				}
				catch (Throwable t) { //NOSONAR
					this.start.countDown();
					handleStartupFailure(this.consumer.getBackOffExecution());
					throw t;
				}

				if (getTransactionManager() != null) {
					/*
					 * Register the consumer's channel so it will be used by the transaction manager
					 * if it's an instance of RabbitTransactionManager.
					 */
					ConsumerChannelRegistry.registerConsumerChannel(this.consumer.getChannel(), getConnectionFactory());
				}

				while (isActive(this.consumer) || this.consumer.hasDelivery() || !this.consumer.cancelled()) {
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
						long idleEventInterval = getIdleEventInterval();
						if (idleEventInterval > 0) {
							if (receivedOk) {
								updateLastReceive();
							}
							else {
								long now = System.currentTimeMillis();
								long lastAlertAt = SimpleMessageListenerContainer.this.lastNoMessageAlert.get();
								long lastReceive = getLastReceive();
								if (now > lastReceive + idleEventInterval
										&& now > lastAlertAt + idleEventInterval
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
				logger.error("Consumer received fatal=" + isMismatchedQueuesFatal() + " exception on startup", ex);
				if (isMissingQueuesFatal()) {
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
			catch (PossibleAuthenticationFailureException ex) {
				logger.error("Consumer received fatal=" + isPossibleAuthenticationFailureFatal() +
						" exception during processing", ex);
				if (isPossibleAuthenticationFailureFatal()) {
					this.startupException =
							new FatalListenerStartupException("Authentication failure",
									new AmqpAuthenticationException(ex));
					// Fatal, but no point re-throwing, so just abort.
					aborted = true;
				}
				publishConsumerFailedEvent("Consumer received PossibleAuthenticationFailure during startup", aborted, ex);
			}
			catch (ShutdownSignalException e) {
				if (RabbitUtils.isNormalShutdown(e)) {
					if (logger.isDebugEnabled()) {
						logger.debug("Consumer received Shutdown Signal, processing stopped: " + e.getMessage());
					}
				}
				else {
					logConsumerException(e);
				}
			}
			catch (AmqpIOException e) {
				if (e.getCause() instanceof IOException && e.getCause().getCause() instanceof ShutdownSignalException
						&& e.getCause().getCause().getMessage().contains("in exclusive use")) {
					getExclusiveConsumerExceptionLogger().log(logger,
							"Exclusive consumer failure", e.getCause().getCause());
					publishConsumerFailedEvent("Consumer raised exception, attempting restart", false, e);
				}
				else {
					logConsumerException(e);
				}
			}
			catch (Error e) { //NOSONAR
				// ok to catch Error - we're aborting so will stop
				logger.error("Consumer thread error, thread abort.", e);
				publishConsumerFailedEvent("Consumer threw an Error", true, e);
				aborted = true;
			}
			catch (Throwable t) { //NOSONAR
				// by now, it must be an exception
				if (isActive()) {
					logConsumerException(t);
				}
			}
			finally {
				if (getTransactionManager() != null) {
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
					if (getApplicationEventPublisher() != null) {
						getApplicationEventPublisher().publishEvent(
								new AsyncConsumerStoppedEvent(SimpleMessageListenerContainer.this, this.consumer));
					}
				}
				catch (AmqpException e) {
					logger.info("Could not cancel message consumer", e);
				}
				if (aborted && SimpleMessageListenerContainer.this.containerStoppingForAbort
						.compareAndSet(null, Thread.currentThread())) {
					logger.error("Stopping container from aborted consumer");
					stop();
					SimpleMessageListenerContainer.this.containerStoppingForAbort.set(null);
					ListenerContainerConsumerFailedEvent event = null;
					do {
						try {
							event = SimpleMessageListenerContainer.this.abortEvents.poll(5, TimeUnit.SECONDS);
							if (event != null) {
								SimpleMessageListenerContainer.this.publishConsumerFailedEvent(
										event.getReason(), event.isFatal(), event.getThrowable());
							}
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
					while (event != null);
				}
			}
			else {
				logger.info("Restarting " + this.consumer);
				restart(this.consumer);
			}

			if (routingLookupKey != null) {
				SimpleResourceHolder.unbind(getRoutingConnectionFactory());
			}
		}

		private void logConsumerException(Throwable t) {
			if (logger.isDebugEnabled()
					|| !(t instanceof AmqpConnectException || t instanceof ConsumerCancelledException)) {
				logger.debug(
						"Consumer raised exception, processing can restart if the connection factory supports it",
						t);
			}
			else {
				if (t instanceof ConsumerCancelledException && this.consumer.isNormalCancel()) {
					if (logger.isDebugEnabled()) {
						logger.debug(
								"Consumer raised exception, processing can restart if the connection factory supports it. "
										+ "Exception summary: " + t);
					}
				}
				else if (logger.isWarnEnabled()) {
					logger.warn(
							"Consumer raised exception, processing can restart if the connection factory supports it. "
									+ "Exception summary: " + t);
				}
			}
			publishConsumerFailedEvent("Consumer raised exception, attempting restart", false, t);
		}

	}

}
