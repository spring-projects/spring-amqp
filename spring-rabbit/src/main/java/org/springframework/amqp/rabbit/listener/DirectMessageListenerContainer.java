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

package org.springframework.amqp.rabbit.listener;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;

import org.springframework.amqp.AmqpApplicationContextClosedException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.ConsumerChannelRegistry;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.connection.SimpleResourceHolder;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.ObjectUtils;
import org.springframework.util.backoff.BackOffExecution;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * The {@code SimpleMessageListenerContainer} is not so simple. Recent changes to the
 * rabbitmq java client has facilitated a much simpler listener container that invokes the
 * listener directly on the rabbit client consumer thread. There is no txSize property -
 * each message is acked (or nacked) individually.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Nicolas Ristock
 *
 * @since 2.0
 *
 */
public class DirectMessageListenerContainer extends AbstractMessageListenerContainer {

	private static final int DEFAULT_MONITOR_INTERVAL = 10_000;

	private static final int DEFAULT_ACK_TIMEOUT = 20_000;

	protected final List<SimpleConsumer> consumers = new LinkedList<>(); // NOSONAR

	private final List<SimpleConsumer> consumersToRestart = new LinkedList<>();

	private final MultiValueMap<String, SimpleConsumer> consumersByQueue = new LinkedMultiValueMap<>();

	private final ActiveObjectCounter<SimpleConsumer> cancellationLock = new ActiveObjectCounter<>();

	private TaskScheduler taskScheduler;

	private boolean taskSchedulerSet;

	private long monitorInterval = DEFAULT_MONITOR_INTERVAL;

	private int messagesPerAck;

	private long ackTimeout = DEFAULT_ACK_TIMEOUT;

	private volatile boolean started;

	private volatile boolean aborted;

	private volatile boolean hasStopped;

	private volatile CountDownLatch startedLatch = new CountDownLatch(1);

	private volatile int consumersPerQueue = 1;

	private volatile ScheduledFuture<?> consumerMonitorTask;

	private volatile long lastAlertAt;

	private volatile long lastRestartAttempt;

	/**
	 * Create an instance; {@link #setConnectionFactory(ConnectionFactory)} must
	 * be called before starting.
	 */
	public DirectMessageListenerContainer() {
		setMissingQueuesFatal(false);
	}

	/**
	 * Create an instance with the provided connection factory.
	 * @param connectionFactory the connection factory.
	 */
	public DirectMessageListenerContainer(ConnectionFactory connectionFactory) {
		setConnectionFactory(connectionFactory);
		setMissingQueuesFatal(false);
	}

	/**
	 * Each queue runs in its own consumer; set this property to create multiple
	 * consumers for each queue.
	 * If the container is already running, the number of consumers per queue will
	 * be adjusted up or down as necessary.
	 * @param consumersPerQueue the consumers per queue.
	 */
	public void setConsumersPerQueue(int consumersPerQueue) {
		if (isRunning()) {
			adjustConsumers(consumersPerQueue);
		}
		this.consumersPerQueue = consumersPerQueue;
	}

	/**
	 * Set to true for an exclusive consumer - if true, the
	 * {@link #setConsumersPerQueue(int) consumers per queue} must be 1.
	 * @param exclusive true for an exclusive consumer.
	 */
	@Override
	public final void setExclusive(boolean exclusive) {
		Assert.isTrue(!exclusive || (this.consumersPerQueue == 1),
				"When the consumer is exclusive, the consumers per queue must be 1");
		super.setExclusive(exclusive);
	}

	/**
	 * Set the task scheduler to use for the task that monitors idle containers and
	 * failed consumers.
	 * @param taskScheduler the scheduler.
	 */
	public void setTaskScheduler(TaskScheduler taskScheduler) {
		this.taskScheduler = taskScheduler;
		this.taskSchedulerSet = true;
	}

	/**
	 * Set how often to run a task to check for failed consumers and idle containers.
	 * @param monitorInterval the interval; default 10000 but it will be adjusted down
	 * to the smallest of this, {@link #setIdleEventInterval(long) idleEventInterval} / 2
	 * (if configured) or
	 * {@link #setFailedDeclarationRetryInterval(long) failedDeclarationRetryInterval}.
	 */
	public void setMonitorInterval(long monitorInterval) {
		this.monitorInterval = monitorInterval;
	}

	@Override
	public void setQueueNames(String... queueName) {
		Assert.state(!isRunning(), "Cannot set queue names while running, use add/remove");
		super.setQueueNames(queueName);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Defaults to false for this container.
	 */
	@Override
	public final void setMissingQueuesFatal(boolean missingQueuesFatal) {
		super.setMissingQueuesFatal(missingQueuesFatal);
	}

	/**
	 * Set the number of messages to receive before acknowledging (success).
	 * A failed message will short-circuit this counter.
	 * @param messagesPerAck the number of messages.
	 * @see #setAckTimeout(long)
	 */
	public void setMessagesPerAck(int messagesPerAck) {
		this.messagesPerAck = messagesPerAck;
	}

	/**
	 * An approximate timeout; when {@link #setMessagesPerAck(int) messagesPerAck} is
	 * greater than 1, and this time elapses since the last ack, the pending acks will be
	 * sent either when the next message arrives, or a short time later if no additional
	 * messages arrive. In that case, the actual time depends on the
	 * {@link #setMonitorInterval(long) monitorInterval}.
	 * @param ackTimeout the timeout in milliseconds (default 20000);
	 * @see #setMessagesPerAck(int)
	 */
	public void setAckTimeout(long ackTimeout) {
		this.ackTimeout = ackTimeout;
	}

	@Override
	public void addQueueNames(String... queueNames) {
		try {
			addQueues(Arrays.stream(queueNames));
		}
		catch (AmqpIOException e) {
			throw new AmqpIOException("Failed to add " + Arrays.asList(queueNames), e.getCause());
		}
		super.addQueueNames(queueNames);
	}

	private void addQueues(Stream<String> queueNameStream) {
		if (isRunning()) {
			synchronized (this.consumersMonitor) {
				checkStartState();
				Set<String> current = getQueueNamesAsSet();
				queueNameStream.forEach(queue -> {
					if (current.contains(queue)) {
						this.logger.warn("Queue " + queue + " is already configured for this container: "
								+ this + ", ignoring add");
					}
					else {
						consumeFromQueue(queue);
					}
				});
			}
		}
	}

	@Override
	public boolean removeQueueNames(String... queueNames) {
		removeQueues(Arrays.stream(queueNames));
		return super.removeQueueNames(queueNames);
	}

	private void removeQueues(Stream<String> queueNames) {
		if (isRunning()) {
			synchronized (this.consumersMonitor) {
				checkStartState();
				queueNames.map(this.consumersByQueue::remove)
						.filter(Objects::nonNull)
						.flatMap(Collection::stream)
						.forEach(this::cancelConsumer);
			}
		}
	}

	private void adjustConsumers(int newCount) {
		synchronized (this.consumersMonitor) {
			checkStartState();
			this.consumersToRestart.clear();
			for (String queue : getQueueNames()) {
				while (this.consumersByQueue.get(queue) == null
						|| this.consumersByQueue.get(queue).size() < newCount) {
					doConsumeFromQueue(queue);
				}
				List<SimpleConsumer> consumerList = this.consumersByQueue.get(queue);
				if (consumerList != null && consumerList.size() > newCount) {
					IntStream.range(newCount, consumerList.size())
							.mapToObj(i -> consumerList.remove(0))
							.forEach(this::cancelConsumer);
				}
			}
		}
	}

	private void checkStartState() {
		if (!this.isRunning()) {
			try {
				Assert.state(this.startedLatch.await(60, TimeUnit.SECONDS),
						"Container is not started - cannot adjust queues");
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AmqpException("Interrupted waiting for start", e);
			}
		}
	}

	@Override
	protected void doInitialize() throws Exception {
		if (this.taskScheduler == null) {
			ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
			threadPoolTaskScheduler.setThreadNamePrefix(getBeanName() + "-consumerMonitor-");
			threadPoolTaskScheduler.afterPropertiesSet();
			this.taskScheduler = threadPoolTaskScheduler;
		}
		if (this.messagesPerAck > 0) {
			Assert.state(!isChannelTransacted(), "'messagesPerAck' is not allowed with transactions");
		}
	}

	@Override
	protected void doStart() throws Exception {
		if (!this.started) {
			actualStart();
		}
	}

	@Override
	protected void doStop() {
		super.doStop();
		if (!this.taskSchedulerSet && this.taskScheduler != null) {
			((ThreadPoolTaskScheduler) this.taskScheduler).shutdown();
			this.taskScheduler = null;
		}
	}

	protected void actualStart() throws Exception {
		this.aborted = false;
		this.hasStopped = false;
		if (getPrefetchCount() < this.messagesPerAck) {
			setPrefetchCount(this.messagesPerAck);
		}
		super.doStart();
		final String[] queueNames = getQueueNames();
		checkMissingQueues(queueNames);
		if (getTaskExecutor() == null) {
			afterPropertiesSet();
		}
		long idleEventInterval = getIdleEventInterval();
		if (this.taskScheduler == null) {
			afterPropertiesSet();
		}
		if (idleEventInterval > 0 && this.monitorInterval > idleEventInterval) {
			this.monitorInterval = idleEventInterval / 2;
		}
		if (getFailedDeclarationRetryInterval() < this.monitorInterval) {
			this.monitorInterval = getFailedDeclarationRetryInterval();
		}
		this.lastRestartAttempt = System.currentTimeMillis();
		this.consumerMonitorTask = this.taskScheduler.scheduleAtFixedRate(() -> {
			long now = System.currentTimeMillis();
			if (idleEventInterval > 0) {
				if (now - getLastReceive() > idleEventInterval && now - this.lastAlertAt > idleEventInterval) {
					publishIdleContainerEvent(now - getLastReceive());
					this.lastAlertAt = now;
				}
			}
			final List<SimpleConsumer> consumersToCancel;
			synchronized (this.consumersMonitor) {
				consumersToCancel = this.consumers.stream()
						.filter(c -> {
							boolean open = c.getChannel().isOpen();
							if (open && this.messagesPerAck > 1) {
								try {
									c.ackIfNecessary(now);
								}
								catch (IOException e) {
									this.logger.error("Exception while sending delayed ack", e);
								}
							}
							return !open;
						})
						.collect(Collectors.toList());
			}
			consumersToCancel
					.forEach(c -> {
						try {
							RabbitUtils.closeMessageConsumer(c.getChannel(),
									Collections.singletonList(c.getConsumerTag()), isChannelTransacted());
						}
						catch (Exception e) {
							if (logger.isDebugEnabled()) {
								logger.debug("Error closing consumer " + c, e);
							}
						}
						this.logger.error("Consumer canceled - channel closed " + c);
						c.cancelConsumer("Consumer " + c + " channel closed");
					});
			if (this.lastRestartAttempt + getFailedDeclarationRetryInterval() < now) {
				synchronized (this.consumersMonitor) {
					List<SimpleConsumer> restartableConsumers = new ArrayList<>(this.consumersToRestart);
					this.consumersToRestart.clear();
					if (this.started) {
						if (restartableConsumers.size() > 0) {
							doRedeclareElementsIfNecessary();
						}
						for (SimpleConsumer consumer : restartableConsumers) {
							if (this.logger.isDebugEnabled() && restartableConsumers.size() > 0) {
								this.logger.debug("Attempting to restart consumer " + consumer);
							}
							try {
								doConsumeFromQueue(consumer.getQueue());
							}
							catch (AmqpConnectException | AmqpIOException e) {
								this.logger.error("Cannot connect to server", e);
								if (e.getCause() instanceof AmqpApplicationContextClosedException) {
									this.logger.error("Application context is closed, terminating");
									this.taskScheduler.schedule(this::stop, new Date());
								}
								break;
							}
						}
						this.lastRestartAttempt = now;
					}
				}
			}
			processMonitorTask();
		}, this.monitorInterval);
		if (queueNames.length > 0) {
			doRedeclareElementsIfNecessary();
			getTaskExecutor().execute(() -> {

				synchronized (this.consumersMonitor) {
					if (this.hasStopped) { // container stopped before we got the lock
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("Consumer start aborted - container stopping");
						}
					}
					else {
						BackOffExecution backOffExecution = getRecoveryBackOff().start();
						while (!DirectMessageListenerContainer.this.started && isRunning()) {
							this.cancellationLock.reset();
							try {
								for (String queue : queueNames) {
									consumeFromQueue(queue);
								}
							}
							catch (AmqpConnectException | AmqpIOException e) {
								long nextBackOff = backOffExecution.nextBackOff();
								if (nextBackOff < 0 || e.getCause() instanceof AmqpApplicationContextClosedException) {
									DirectMessageListenerContainer.this.aborted = true;
									shutdown();
									this.logger.error("Failed to start container - fatal error or backOffs exhausted",
											e);
									this.taskScheduler.schedule(this::stop, new Date());
									break;
								}
								this.logger.error("Error creating consumer; retrying in " + nextBackOff, e);
								doShutdown();
								try {
									Thread.sleep(nextBackOff);
								}
								catch (InterruptedException e1) {
									Thread.currentThread().interrupt();
								}
								continue; // initialization failed; try again having rested for backOff-interval
							}
							DirectMessageListenerContainer.this.started = true;
							DirectMessageListenerContainer.this.startedLatch.countDown();
						}
					}
				}

			});
		}
		else {
			this.started = true;
			this.startedLatch.countDown();
		}
		if (logger.isInfoEnabled()) {
			this.logger.info("Container initialized for queues: " + Arrays.asList(queueNames));
		}
	}

	protected void doRedeclareElementsIfNecessary() {
		String routingLookupKey = getRoutingLookupKey();
		if (routingLookupKey != null) {
			SimpleResourceHolder.bind(getRoutingConnectionFactory(), routingLookupKey);
		}
		try {
			redeclareElementsIfNecessary();
		}
		catch (Exception e) {
			this.logger.error("Failed to redeclare elements", e);
		}
		finally {
			if (routingLookupKey != null) {
				SimpleResourceHolder.unbind(getRoutingConnectionFactory());
			}
		}
	}

	/**
	 * Subclasses can override this to take additional actions when the monitor task runs.
	 */
	protected void processMonitorTask() {
		// default do nothing
	}

	private void checkMissingQueues(String[] queueNames) {
		if (isMissingQueuesFatal()) {
			AmqpAdmin checkAdmin = getAmqpAdmin();
			if (checkAdmin == null) {
				/*
				 * Checking queue existence doesn't require an admin in the context or injected into
				 * the container. If there's no such admin, just create a local one here.
				 * Use reflection to avoid class tangles.
				 */
				try {
					Class<?> clazz = ClassUtils.forName("org.springframework.amqp.rabbit.core.RabbitAdmin",
							getClass().getClassLoader());

					@SuppressWarnings("unchecked")
					Constructor<AmqpAdmin> ctor = (Constructor<AmqpAdmin>) clazz
							.getConstructor(ConnectionFactory.class);
					checkAdmin = ctor.newInstance(getConnectionFactory());
					setAmqpAdmin(checkAdmin);
				}
				catch (Exception e) {
					this.logger.error("Failed to create a RabbitAdmin", e);
				}
			}
			if (checkAdmin != null) {
				for (String queue : queueNames) {
					Properties queueProperties = checkAdmin.getQueueProperties(queue);
					if (queueProperties == null && isMissingQueuesFatal()) {
						throw new IllegalStateException("At least one of the configured queues is missing");
					}
				}
			}
		}
	}

	private void consumeFromQueue(String queue) {
		List<SimpleConsumer> list = this.consumersByQueue.get(queue);
		// Possible race with setConsumersPerQueue and the task launched by start()
		if (CollectionUtils.isEmpty(list)) {
			for (int i = 0; i < this.consumersPerQueue; i++) {
				doConsumeFromQueue(queue);
			}
		}
	}

	private void doConsumeFromQueue(String queue) {
		if (!isActive()) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Consume from queue " + queue + " ignore, container stopping");
			}
			return;
		}
		String routingLookupKey = getRoutingLookupKey();
		if (routingLookupKey != null) {
			SimpleResourceHolder.bind(getRoutingConnectionFactory(), routingLookupKey);
		}
		Connection connection = null; // NOSONAR (close)
		try {
			connection = getConnectionFactory().createConnection();
		}
		catch (Exception e) {
			addConsumerToRestart(new SimpleConsumer(null, null, queue));
			throw e instanceof AmqpConnectException
					? (AmqpConnectException) e
					: new AmqpConnectException(e);
		}
		finally {
			if (routingLookupKey != null) {
				SimpleResourceHolder.unbind(getRoutingConnectionFactory());
			}
		}
		Channel channel = null;
		SimpleConsumer consumer = null;
		try {
			channel = connection.createChannel(isChannelTransacted());
			channel.basicQos(getPrefetchCount());
			consumer = new SimpleConsumer(connection, channel, queue);
			channel.queueDeclarePassive(queue);
			consumer.consumerTag = channel.basicConsume(queue, getAcknowledgeMode().isAutoAck(),
					(getConsumerTagStrategy() != null
							? getConsumerTagStrategy().createConsumerTag(queue) : ""),
					isNoLocal(), isExclusive(), getConsumerArguments(), consumer);
		}
		catch (AmqpApplicationContextClosedException e) {
			throw new AmqpConnectException(e);
		}
		catch (IOException | AmqpConnectException e) {
			RabbitUtils.closeChannel(channel);
			RabbitUtils.closeConnection(connection);

			if (e.getCause() instanceof ShutdownSignalException
					&& e.getCause().getMessage().contains("in exclusive use")) {
				getExclusiveConsumerExceptionLogger().log(logger,
						"Exclusive consumer failure", e.getCause());
				publishConsumerFailedEvent("Consumer raised exception, attempting restart", false, e);
			}
			else if (e.getCause() instanceof ShutdownSignalException
					&& RabbitUtils.isPassiveDeclarationChannelClose((ShutdownSignalException) e.getCause())) {
				this.logger.error("Queue not present, scheduling consumer " + consumer + " for restart", e);
			}
			else if (this.logger.isWarnEnabled()) {
				this.logger.warn("basicConsume failed, scheduling consumer " + consumer + " for restart", e);
			}

			if (consumer == null) {
				addConsumerToRestart(new SimpleConsumer(null, null, queue));
			}
			else {
				addConsumerToRestart(consumer);
				consumer = null;
			}
		}
		synchronized (this.consumersMonitor) {
			if (consumer != null) {
				this.cancellationLock.add(consumer);
				this.consumers.add(consumer);
				this.consumersByQueue.add(queue, consumer);
				if (this.logger.isInfoEnabled()) {
					this.logger.info(consumer + " started");
				}
				if (getApplicationEventPublisher() != null) {
					getApplicationEventPublisher().publishEvent(new AsyncConsumerStartedEvent(this, consumer));
				}
			}
		}
	}

	@Override
	protected void doShutdown() {
		LinkedList<SimpleConsumer> canceledConsumers = null;
		boolean waitForConsumers = false;
		synchronized (this.consumersMonitor) {
			if (this.started || this.aborted) {
				// Copy in the same order to avoid ConcurrentModificationException during remove in the cancelConsumer().
				canceledConsumers = new LinkedList<>(this.consumers);
				actualShutDown(canceledConsumers);
				waitForConsumers = true;
			}
		}
		if (waitForConsumers) {
			try {
				if (this.cancellationLock.await(getShutdownTimeout(), TimeUnit.MILLISECONDS)) {
					this.logger.info("Successfully waited for consumers to finish.");
				}
				else {
					this.logger.info("Consumers not finished.");
					if (isForceCloseChannel()) {
						canceledConsumers.forEach(consumer -> {
							String eventMessage = "Closing channel for unresponsive consumer: " + consumer;
							if (logger.isWarnEnabled()) {
								logger.warn(eventMessage);
							}
							consumer.cancelConsumer(eventMessage);
						});
					}
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				this.logger.warn("Interrupted waiting for consumers. Continuing with shutdown.");
			}
			finally {
				this.startedLatch = new CountDownLatch(1);
				this.started = false;
				this.aborted = false;
				this.hasStopped = true;
			}
		}
	}

	/**
	 * Must hold this.consumersMonitor.
	 * @param consumers a copy of this.consumers.
	 */
	private void actualShutDown(List<SimpleConsumer> consumers) {
		Assert.state(getTaskExecutor() != null, "Cannot shut down if not initialized");
		this.logger.debug("Shutting down");
		consumers.forEach(this::cancelConsumer);
		this.consumers.clear();
		this.consumersByQueue.clear();
		this.logger.debug("All consumers canceled");
		if (this.consumerMonitorTask != null) {
			this.consumerMonitorTask.cancel(true);
			this.consumerMonitorTask = null;
		}
	}

	private void cancelConsumer(SimpleConsumer consumer) {
		try {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Canceling " + consumer);
			}
			synchronized (consumer) {
				consumer.canceled = true;
				if (this.messagesPerAck > 1) {
					consumer.ackIfNecessary(0L);
				}
			}
			consumer.getChannel().basicCancel(consumer.getConsumerTag());
		}
		catch (IOException e) {
			this.logger.error("Failed to cancel consumer: " + consumer, e);
		}
		finally {
			this.consumers.remove(consumer);
			consumerRemoved(consumer);
		}
	}

	private void addConsumerToRestart(SimpleConsumer consumer) {
		if (this.started) {
			this.consumersToRestart.add(consumer);
		}
	}

	/**
	 * Called whenever a consumer is removed.
	 * @param consumer the consumer.
	 */
	protected void consumerRemoved(SimpleConsumer consumer) {
		// default empty
	}

	/**
	 * The consumer object.
	 */
	final class SimpleConsumer extends DefaultConsumer {

		private final Log logger = DirectMessageListenerContainer.this.logger;

		private final Connection connection;

		private final String queue;

		private final boolean ackRequired;

		private final ConnectionFactory connectionFactory = getConnectionFactory();

		private final PlatformTransactionManager transactionManager = getTransactionManager();

		private final TransactionAttribute transactionAttribute = getTransactionAttribute();

		private final boolean isRabbitTxManager = this.transactionManager instanceof RabbitTransactionManager;

		private final int messagesPerAck = DirectMessageListenerContainer.this.messagesPerAck;

		private final long ackTimeout = DirectMessageListenerContainer.this.ackTimeout;

		private int pendingAcks;

		private long lastAck = System.currentTimeMillis();

		private long lastDeliveryComplete = this.lastAck;

		private long deliveryTag;

		private volatile String consumerTag;

		private volatile int epoch;

		private volatile TransactionTemplate transactionTemplate;

		private volatile boolean canceled;

		private SimpleConsumer(Connection connection, Channel channel, String queue) {
			super(channel);
			this.connection = connection;
			this.queue = queue;
			this.ackRequired = !getAcknowledgeMode().isAutoAck() && !getAcknowledgeMode().isManual();
		}

		private String getQueue() {
			return this.queue;
		}

		@Override
		public String getConsumerTag() {
			return this.consumerTag;
		}

		/**
		 * Return the current epoch for this consumer; consumersMonitor must be held.
		 * @return the epoch.
		 */
		int getEpoch() {
			return this.epoch;
		}

		/**
		 * Increment and return the current epoch for this consumer; consumersMonitor must
		 * be held.
		 * @return the epoch.
		 */
		int incrementAndGetEpoch() {
			return ++this.epoch;
		}

		@Override
		public void handleDelivery(String consumerTag, Envelope envelope,
				BasicProperties properties, byte[] body) throws IOException {
			MessageProperties messageProperties =
					getMessagePropertiesConverter().toMessageProperties(properties, envelope, "UTF-8");
			messageProperties.setConsumerTag(consumerTag);
			messageProperties.setConsumerQueue(this.queue);
			Message message = new Message(body, messageProperties);
			long deliveryTag = envelope.getDeliveryTag();
			if (this.logger.isDebugEnabled()) {
				this.logger.debug(this + " received " + message);
			}
			updateLastReceive();
			if (this.transactionManager != null) {
				try {
					if (this.isRabbitTxManager) {
						ConsumerChannelRegistry.registerConsumerChannel(getChannel(), this.connectionFactory);
					}
					if (this.transactionTemplate == null) {
						this.transactionTemplate =
								new TransactionTemplate(this.transactionManager, this.transactionAttribute);
					}
					this.transactionTemplate.execute(s -> {
						RabbitResourceHolder resourceHolder = ConnectionFactoryUtils.bindResourceToTransaction(
								new RabbitResourceHolder(getChannel(), false), this.connectionFactory, true);
						if (resourceHolder != null) {
							resourceHolder.addDeliveryTag(getChannel(), deliveryTag);
						}
						// unbound in ResourceHolderSynchronization.beforeCompletion()
						try {
							callExecuteListener(message, deliveryTag);
						}
						catch (RuntimeException e1) {
							prepareHolderForRollback(resourceHolder, e1);
							throw e1;
						}
						catch (Throwable e2) { //NOSONAR ok to catch Throwable here because we re-throw it below
							throw new WrappedTransactionException(e2);
						}
						return null;
					});
				}
				catch (Throwable e) { // NOSONAR - errors are rethrown
					if (e instanceof WrappedTransactionException) {
						if (e.getCause() instanceof Error) {
							throw (Error) e.getCause();
						}
					}
				}
				finally {
					if (this.isRabbitTxManager) {
						ConsumerChannelRegistry.unRegisterConsumerChannel();
					}
				}
			}
			else {
				try {
					callExecuteListener(message, deliveryTag);
				}
				catch (Exception e) {
					// NOSONAR
				}
			}
		}

		private void callExecuteListener(Message message, long deliveryTag) throws Exception {
			boolean channelLocallyTransacted = isChannelLocallyTransacted();
			try {
				executeListener(getChannel(), message);
				handleAck(deliveryTag, channelLocallyTransacted);
			}
			catch (ImmediateAcknowledgeAmqpException e) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("User requested ack for failed delivery '"
							+ e.getMessage() + "': "
							+ deliveryTag);
				}
				handleAck(deliveryTag, channelLocallyTransacted);
			}
			catch (Exception e) {
				if (causeChainHasImmediateAcknowledgeAmqpException(e)) {
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("User requested ack for failed delivery: " + deliveryTag);
					}
					handleAck(deliveryTag, channelLocallyTransacted);
				}
				else {
					this.logger.error("Failed to invoke listener", e);
					if (this.transactionManager != null) {
						if (this.transactionAttribute.rollbackOn(e)) {
							RabbitResourceHolder resourceHolder = (RabbitResourceHolder) TransactionSynchronizationManager
									.getResource(getConnectionFactory());
							if (resourceHolder == null) {
								/*
								 * If we don't actually have a transaction, we have to roll back
								 * manually. See prepareHolderForRollback().
								 */
								rollback(deliveryTag, e);
							}
							throw e; // encompassing transaction will handle the rollback.
						}
						else {
							if (this.logger.isDebugEnabled()) {
								this.logger.debug("No rollback for " + e);
							}
						}
					}
					else {
						rollback(deliveryTag, e);
						// no need to rethrow e - we'd ignore it anyway, not throw to client
					}
				}
			}
		}

		private void handleAck(long deliveryTag, boolean channelLocallyTransacted) throws IOException {
			/*
			 * If we have a TX Manager, but no TX, act like we are locally transacted.
			 */
			boolean isLocallyTransacted =
					channelLocallyTransacted ||
							(isChannelTransacted() &&
									TransactionSynchronizationManager.getResource(this.connectionFactory) == null);
			try {
				if (this.ackRequired) {
					if (this.messagesPerAck > 1) {
						synchronized (this) {
							this.deliveryTag = deliveryTag;
							this.pendingAcks++;
							this.lastDeliveryComplete = System.currentTimeMillis();
							ackIfNecessary(this.lastDeliveryComplete);
						}
					}
					else if (!isChannelTransacted() || isLocallyTransacted) {
						getChannel().basicAck(deliveryTag, false);
					}
				}
				if (isLocallyTransacted) {
					RabbitUtils.commitIfNecessary(getChannel());
				}
			}
			catch (Exception e) {
				this.logger.error("Error acking", e);
			}
		}

		/**
		 * Send the ack if we've reached the threshold (count or time) or immediately if
		 * we are processing deliveries after a cancel has been issued.
		 * @param now the current time.
		 * @throws IOException if one occurs.
		 */
		private synchronized void ackIfNecessary(long now) throws IOException {
			if (this.pendingAcks >= this.messagesPerAck || (
					this.pendingAcks > 0 && (now - this.lastAck > this.ackTimeout || this.canceled))) {
				sendAck(now);
			}
		}

		private void rollback(long deliveryTag, Exception e) {
			if (isChannelTransacted()) {
				RabbitUtils.rollbackIfNecessary(getChannel());
			}
			if (this.ackRequired) {
				try {
					if (this.messagesPerAck > 1) {
						synchronized (this) {
							if (this.pendingAcks > 0) {
								sendAck(System.currentTimeMillis());
							}
						}
					}
					getChannel().basicNack(deliveryTag, true,
							ContainerUtils.shouldRequeue(isDefaultRequeueRejected(), e, this.logger));
				}
				catch (IOException e1) {
					this.logger.error("Failed to nack message", e1);
				}
			}
			if (isChannelTransacted()) {
				RabbitUtils.commitIfNecessary(getChannel());
			}
		}

		protected synchronized void sendAck(long now) throws IOException {
			getChannel().basicAck(this.deliveryTag, true);
			this.lastAck = now;
			this.pendingAcks = 0;
		}

		@Override
		public void handleConsumeOk(String consumerTag) {
			super.handleConsumeOk(consumerTag);
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("New " + this + " consumeOk");
			}
			if (getApplicationEventPublisher() != null) {
				getApplicationEventPublisher().publishEvent(new ConsumeOkEvent(this, getQueue(), consumerTag));
			}
		}

		@Override
		public void handleCancelOk(String consumerTag) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("CancelOk " + this);
			}
			finalizeConsumer();
		}

		@Override
		public void handleCancel(String consumerTag) throws IOException {
			this.logger.error("Consumer canceled - queue deleted? " + this);
			cancelConsumer("Consumer " + this + " canceled");
		}

		void cancelConsumer(final String eventMessage) {
			publishConsumerFailedEvent(eventMessage, true, null);
			synchronized (DirectMessageListenerContainer.this.consumersMonitor) {
				List<SimpleConsumer> list = DirectMessageListenerContainer.this.consumersByQueue.get(this.queue);
				if (list != null) {
					list.remove(this);
				}
				DirectMessageListenerContainer.this.consumers.remove(this);
				addConsumerToRestart(this);
			}
			finalizeConsumer();
		}

		private void finalizeConsumer() {
			RabbitUtils.setPhysicalCloseRequired(getChannel(), true);
			RabbitUtils.closeChannel(getChannel());
			RabbitUtils.closeConnection(this.connection);
			DirectMessageListenerContainer.this.cancellationLock.release(this);
			consumerRemoved(this);
		}

		@Override
		public String toString() {
			return "SimpleConsumer [queue=" + this.queue + ", consumerTag=" + this.consumerTag
					+ " identity=" + ObjectUtils.getIdentityHexString(this) + "]";
		}

	}

}
