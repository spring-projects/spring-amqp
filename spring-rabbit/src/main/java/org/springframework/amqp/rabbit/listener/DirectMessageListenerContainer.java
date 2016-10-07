/*
 * Copyright 2016 the original author or authors.
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
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.ConsumerChannelRegistry;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
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
 *
 * @since 2.0
 *
 */
public class DirectMessageListenerContainer extends AbstractMessageListenerContainer {

	private static final int DEFAULT_MONITOR_INTERVAL = 10000;

	protected final List<SimpleConsumer> consumers = new LinkedList<>(); // NOSONAR

	private final List<SimpleConsumer> consumersToRestart = new LinkedList<>();

	private final MultiValueMap<String, SimpleConsumer> consumersByQueue = new LinkedMultiValueMap<>();

	private final ActiveObjectCounter<SimpleConsumer> cancellationLock = new ActiveObjectCounter<>();

	private TaskScheduler taskScheduler;

	private long monitorInterval = DEFAULT_MONITOR_INTERVAL;

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

	@Override
	public void setQueues(Queue... queues) {
		Assert.state(!isRunning(), "Cannot set queue names while running, use add/remove");
		super.setQueues(queues);
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

	@Override
	public void addQueues(Queue... queues) {
		try {
			addQueues(Arrays.stream(queues)
					.map(Queue::getName));
		}
		catch (AmqpIOException e) {
			throw new AmqpIOException("Failed to add " + Arrays.asList(queues), e.getCause());
		}
		super.addQueues(queues);
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

	@Override
	public boolean removeQueues(Queue... queues) {
		removeQueues(Arrays.stream(queues)
				.map(Queue::getName));
		return super.removeQueues(queues);
	}

	private void removeQueues(Stream<String> queueNames) {
		if (isRunning()) {
			synchronized (this.consumersMonitor) {
				checkStartState();
				queueNames.map(this.consumersByQueue::remove)
					.filter(consumersOnQueue -> consumersOnQueue != null)
					.flatMap(Collection::stream)
					.forEach(this::cancelConsumer);
			}
		}
	}

	@Override
	protected boolean canRemoveLastQueue() {
		return true;
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
					int currentCount = consumerList.size();
					for (int i = newCount; i < currentCount; i++) {
						SimpleConsumer consumer = consumerList.remove(i);
						cancelConsumer(consumer);
					}
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
	}

	@Override
	protected void doStart() throws Exception {
		if (!this.started) {
			actualStart();
		}
	}

	protected void actualStart() throws Exception {
		this.aborted = false;
		this.hasStopped = false;
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
			this.consumers.stream()
					.filter(c -> !c.getChannel().isOpen())
					.collect(Collectors.toList()) // needed to avoid ConcurrentModificationException in cancelConsumer()
					.forEach(c -> {
						this.logger.error("Consumer canceled - channel closed " + c);
						c.cancelConsumer("Consumer " + c + " channel closed");
					});
			if (this.lastRestartAttempt + getFailedDeclarationRetryInterval() < now) {
				synchronized (this.consumersMonitor) {
					List<SimpleConsumer> restartableConsumers = new ArrayList<>(this.consumersToRestart);
					this.consumersToRestart.clear();
					if (this.started) {
						if (restartableConsumers.size() > 0) {
							redeclareElementsIfNecessary();
						}
						for (SimpleConsumer consumer : restartableConsumers) {
							if (this.logger.isDebugEnabled() && restartableConsumers.size() > 0) {
								logger.debug("Attempting to restart consumer " + consumer);
							}
							doConsumeFromQueue(consumer.getQueue());
						}
						this.lastRestartAttempt = now;
					}
				}
			}
			processMonitorTask();
		}, this.monitorInterval);
		if (queueNames.length > 0) {
			try {
				redeclareElementsIfNecessary();
			}
			catch (Exception e) {
				this.logger.error("Failed to redeclare elements", e);
			}
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
							catch (AmqpConnectException e) {
								long nextBackOff = backOffExecution.nextBackOff();
								if (nextBackOff < 0) {
									DirectMessageListenerContainer.this.aborted = true;
									shutdown();
									this.logger.error("Failed to start container - backOffs exhausted", e);
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

	/**
	 * Subclasses can override this to take additional actions when the monitor task runs.
	 */
	protected void processMonitorTask() {
		// default do nothing
	}

	private void checkMissingQueues(String[] queueNames) {
		if (isMissingQueuesFatal()) {
			RabbitAdmin checkAdmin = getRabbitAdmin();
			if (checkAdmin == null) {
				/*
				 * Checking queue existence doesn't require an admin in the context or injected into
				 * the container. If there's no such admin, just create a local one here.
				 */
				checkAdmin = new RabbitAdmin(getConnectionFactory());
			}
			for (String queue : queueNames) {
				Properties queueProperties = checkAdmin.getQueueProperties(queue);
				if (queueProperties == null && isMissingQueuesFatal()) {
					throw new IllegalStateException("At least one of the configured queues is missing");
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
		Connection connection = null;
		try {
			connection = getConnectionFactory().createConnection();
		}
		catch (Exception e) {
			this.consumersToRestart.add(new SimpleConsumer(null, null, queue));
			throw new AmqpConnectException(e);
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
					false, isExclusive(), getConsumerArguments(), consumer);
		}
		catch (IOException e) {
			if (channel != null) {
				RabbitUtils.closeChannel(channel);
			}
			if (connection != null) {
				RabbitUtils.closeConnection(connection);
			}
			if (e.getCause() instanceof ShutdownSignalException
					&& e.getCause().getMessage().contains("in exclusive use")) {
				getExclusiveConsumerExceptionLogger().log(logger,
						"Exclusive consumer failure", e.getCause());
				publishConsumerFailedEvent("Consumer raised exception, attempting restart", false, e);
			}
			else if (this.logger.isDebugEnabled()) {
				this.logger.debug(
						"Queue not present or basicConsume failed, scheduling consumer " + consumer + " for restart");
			}
			this.consumersToRestart.add(consumer);
			consumer = null;
		}
		synchronized (this.consumersMonitor) {
			if (consumer != null) {
				this.cancellationLock.add(consumer);
				this.consumers.add(consumer);
				this.consumersByQueue.add(queue, consumer);
				if (this.logger.isInfoEnabled()) {
					this.logger.info(consumer + " started");
				}
			}
		}
	}

	@Override
	protected void doShutdown() {
		boolean waitForConsumers = false;
		synchronized (this.consumersMonitor) {
			if (this.started || this.aborted) {
				actualShutDown();
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
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				this.logger.warn("Interrupted waiting for consumers.  Continuing with shutdown.");
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
	 * Must hold this.consumersMonitor
	 */
	private void actualShutDown() {
		Assert.state(getTaskExecutor() != null, "Cannot shut down if not initialized");
		logger.debug("Shutting down");
		// Copy in the same order to avoid ConcurrentModificationException during remove in the cancelConsumer().
		new LinkedList<>(this.consumers).forEach(this::cancelConsumer);
		this.consumers.clear();
		this.consumersByQueue.clear();
		logger.debug("All consumers canceled");
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

	/**
	 * Called whenever a consumer is removed.
	 * @param consumer the consumer.
	 */
	protected void consumerRemoved(SimpleConsumer consumer) {
		// default empty
	}

	final class SimpleConsumer extends DefaultConsumer {

		private final Log logger = DirectMessageListenerContainer.this.logger;

		private final Connection connection;

		private final String queue;

		private final boolean ackRequired;

		private final ConnectionFactory connectionFactory = getConnectionFactory();

		private final PlatformTransactionManager transactionManager = getTransactionManager();

		private final TransactionAttribute transactionAttribute = getTransactionAttribute();

		private final boolean isRabbitTxManager = this.transactionManager instanceof RabbitTransactionManager;

		private volatile String consumerTag;

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
					new TransactionTemplate(this.transactionManager, this.transactionAttribute).execute(s -> {
						ConnectionFactoryUtils.bindResourceToTransaction(new RabbitResourceHolder(getChannel(), false),
								this.connectionFactory, true);
						callExecuteListener(message, deliveryTag);
						return null;
					});
				}
				finally {
					if (this.isRabbitTxManager) {
						ConsumerChannelRegistry.unRegisterConsumerChannel();
					}
				}
			}
			else {
				callExecuteListener(message, deliveryTag);
			}
		}

		private void callExecuteListener(Message message, long deliveryTag) {
			try {
				executeListener(getChannel(), message);
				boolean channelLocallyTransacted = isChannelLocallyTransacted();
				if (this.ackRequired) {
					if (isChannelTransacted() && !channelLocallyTransacted) {

						// Not locally transacted but it is transacted so it
						// could be synchronized with an external transaction
						ConnectionFactoryUtils.registerDeliveryTag(this.connectionFactory, getChannel(), deliveryTag);

					}
					else {
						getChannel().basicAck(deliveryTag, false);
					}
				}
				if (channelLocallyTransacted) {
					RabbitUtils.commitIfNecessary(getChannel());
				}
			}
			catch (Exception e) {
				this.logger.error("Failed to invoke listener", e);
				rollback(deliveryTag, e);
			}
		}

		private void rollback(long deliveryTag, Exception e) {
			if (isChannelTransacted()) {
				RabbitUtils.rollbackIfNecessary(getChannel());
			}
			if (this.ackRequired) {
				try {
					getChannel().basicReject(deliveryTag,
							RabbitUtils.shouldRequeue(isDefaultRequeueRejected(), e, this.logger));
				}
				catch (IOException e1) {
					this.logger.error("Failed to nack message", e1);
				}
			}
			if (isChannelTransacted()) {
				RabbitUtils.commitIfNecessary(getChannel());
			}
		}

		@Override
		public void handleConsumeOk(String consumerTag) {
			super.handleConsumeOk(consumerTag);
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("New " + this + " consumeOk");
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
				DirectMessageListenerContainer.this.consumersToRestart.add(this);
			}
			finalizeConsumer();
		}

		private void finalizeConsumer() {
			RabbitUtils.setPhysicalCloseRequired(true);
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
