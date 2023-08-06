/*
 * Copyright 2016-2023 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;

import org.springframework.amqp.AmqpApplicationContextClosedException;
import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.ConsumerChannelRegistry;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.connection.SimpleResourceHolder;
import org.springframework.amqp.rabbit.listener.support.ContainerUtils;
import org.springframework.amqp.rabbit.support.ActiveObjectCounter;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.lang.Nullable;
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
import org.springframework.util.StringUtils;
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
 * @author Cao Weibo
 *
 * @since 2.0
 *
 */
public class DirectMessageListenerContainer extends AbstractMessageListenerContainer {

	private static final int START_WAIT_TIME = 60;

	private static final int DEFAULT_MONITOR_INTERVAL = 10_000;

	private static final int DEFAULT_ACK_TIMEOUT = 20_000;

	protected final List<SimpleConsumer> consumers = new LinkedList<>(); // NOSONAR

	private final Set<SimpleConsumer> consumersToRestart = new LinkedHashSet<>();

	private final Set<String> removedQueues = ConcurrentHashMap.newKeySet();

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
		doSetPossibleAuthenticationFailureFatal(false);
	}

	/**
	 * Create an instance with the provided connection factory.
	 * @param connectionFactory the connection factory.
	 */
	public DirectMessageListenerContainer(ConnectionFactory connectionFactory) {
		setConnectionFactory(connectionFactory);
		setMissingQueuesFatal(false);
		doSetPossibleAuthenticationFailureFatal(false);
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
		Assert.notNull(queueNames, "'queueNames' cannot be null");
		Assert.noNullElements(queueNames, "'queueNames' cannot contain null elements");
		try {
			Arrays.stream(queueNames).forEach(this.removedQueues::remove);
			addQueues(Arrays.stream(queueNames));
		}
		catch (AmqpIOException e) {
			throw new AmqpIOException("Failed to add " + Arrays.toString(queueNames), e);
		}
		super.addQueueNames(queueNames);
	}

	@Override
	public void addQueues(Queue... queues) {
		Assert.notNull(queues, "'queues' cannot be null");
		Assert.noNullElements(queues, "'queues' cannot contain null elements");
		try {
			Arrays.stream(queues)
				.map(q -> q.getActualName())
				.forEach(this.removedQueues::remove);
			addQueues(Arrays.stream(queues).map(Queue::getName));
		}
		catch (AmqpIOException e) {
			throw new AmqpIOException("Failed to add " + Arrays.toString(queues), e);
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
		removeQueues(Arrays.stream(queues).map(Queue::getActualName));
		return super.removeQueues(queues);
	}

	private void removeQueues(Stream<String> queueNames) {
		if (isRunning()) {
			synchronized (this.consumersMonitor) {
				checkStartState();
				queueNames.map(queue -> {
							this.removedQueues.add(queue);
							return this.consumersByQueue.remove(queue);
						})
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
						|| this.consumersByQueue.get(queue).size() < newCount) { // NOSONAR never null
					List<SimpleConsumer> cBQ = this.consumersByQueue.get(queue);
					int index = 0;
					if (cBQ != null) {
						// find a gap or set the index to the end
						List<Integer> indices = cBQ.stream()
								.map(cons -> cons.getIndex())
								.sorted()
								.collect(Collectors.toList());
						for (index = 0; index < indices.size(); index++) {
							if (index < indices.get(index)) {
								break;
							}
						}
					}
					doConsumeFromQueue(queue, index);
				}
				reduceConsumersIfIdle(newCount, queue);
			}
		}
	}

	private void reduceConsumersIfIdle(int newCount, String queue) {
		List<SimpleConsumer> consumerList = this.consumersByQueue.get(queue);
		if (consumerList != null && consumerList.size() > newCount) {
			int delta = consumerList.size() - newCount;
			for (int i = 0; i < delta; i++) {
				int index = findIdleConsumer();
				if (index >= 0) {
					SimpleConsumer consumer = consumerList.remove(index);
					if (consumer != null) {
						cancelConsumer(consumer);
					}
				}
			}
		}
	}

	/**
	 * When adjusting down, return a consumer that can be canceled. Called while
	 * synchronized on consumersMonitor.
	 * @return the consumer index or -1 if non idle.
	 * @since 2.0.6
	 */
	protected int findIdleConsumer() {
		return 0;
	}

	private void checkStartState() {
		if (!this.isRunning()) {
			try {
				Assert.state(this.startedLatch.await(START_WAIT_TIME, TimeUnit.SECONDS),
						"Container is not started - cannot adjust queues");
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AmqpException("Interrupted waiting for start", e);
			}
		}
	}

	@Override
	protected void doInitialize() {
		if (this.taskScheduler == null) {
			ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
			threadPoolTaskScheduler.setThreadNamePrefix(getListenerId() + "-consumerMonitor-");
			threadPoolTaskScheduler.afterPropertiesSet();
			this.taskScheduler = threadPoolTaskScheduler;
		}
		if (this.messagesPerAck > 0) {
			Assert.state(!isChannelTransacted(), "'messagesPerAck' is not allowed with transactions");
		}
	}

	@Override
	protected void doStart() {
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

	protected void actualStart() {
		this.aborted = false;
		this.hasStopped = false;
		if (getPrefetchCount() < this.messagesPerAck) {
			setPrefetchCount(this.messagesPerAck);
		}
		super.doStart();
		final String[] queueNames = getQueueNames();
		checkMissingQueues(queueNames);
		checkConnect();
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
		final Map<String, Queue> namesToQueues = getQueueNamesToQueues();
		this.lastRestartAttempt = System.currentTimeMillis();
		startMonitor(idleEventInterval, namesToQueues);
		if (queueNames.length > 0) {
			doRedeclareElementsIfNecessary();
			getTaskExecutor().execute(() -> { // NOSONAR never null here
				startConsumers(queueNames);
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

	protected void checkConnect() {
		if (isPossibleAuthenticationFailureFatal()) {
			Connection connection = null;
			try {
				getConnectionFactory().createConnection();
			}
			catch (AmqpAuthenticationException ex) {
				this.logger.debug("Failed to authenticate", ex);
				throw ex;
			}
			catch (Exception ex) { // NOSONAR
			}
			finally {
				if (connection != null) {
					connection.close();
				}
			}
		}
	}

	private void startMonitor(long idleEventInterval, final Map<String, Queue> namesToQueues) {
		this.consumerMonitorTask = this.taskScheduler.scheduleAtFixedRate(() -> {
			long now = System.currentTimeMillis();
			checkIdle(idleEventInterval, now);
			checkConsumers(now);
			if (this.lastRestartAttempt + getFailedDeclarationRetryInterval() < now) {
				synchronized (this.consumersMonitor) {
					if (this.started) {
						List<SimpleConsumer> restartableConsumers = new ArrayList<>(this.consumersToRestart);
						this.consumersToRestart.clear();
						if (restartableConsumers.size() > 0) {
							doRedeclareElementsIfNecessary();
						}
						Iterator<SimpleConsumer> iterator = restartableConsumers.iterator();
						while (iterator.hasNext()) {
							SimpleConsumer consumer = iterator.next();
							iterator.remove();
							if (DirectMessageListenerContainer.this.removedQueues.contains(consumer.getQueue())) {
								if (this.logger.isDebugEnabled()) {
									this.logger.debug("Skipping restart of consumer, queue removed " + consumer);
								}
								continue;
							}
							if (this.logger.isDebugEnabled()) {
								this.logger.debug("Attempting to restart consumer " + consumer);
							}
							if (!restartConsumer(namesToQueues, restartableConsumers, consumer)) {
								break;
							}
						}
						this.lastRestartAttempt = now;
					}
				}
			}
			processMonitorTask();
		}, Duration.ofMillis(this.monitorInterval));
	}

	private void checkIdle(long idleEventInterval, long now) {
		if (idleEventInterval > 0
				&& now - getLastReceive() > idleEventInterval && now - this.lastAlertAt > idleEventInterval) {
			publishIdleContainerEvent(now - getLastReceive());
			this.lastAlertAt = now;
		}
	}

	private void checkConsumers(long now) {
		final List<SimpleConsumer> consumersToCancel;
		synchronized (this.consumersMonitor) {
			consumersToCancel = this.consumers.stream()
					.filter(consumer -> {
						boolean open = consumer.getChannel().isOpen() && !consumer.isAckFailed()
								&& !consumer.targetChanged();
						if (open && this.messagesPerAck > 1) {
							try {
								consumer.ackIfNecessary(now);
							}
							catch (Exception e) {
								this.logger.error("Exception while sending delayed ack", e);
							}
						}
						return !open;
					})
					.collect(Collectors.toList());
		}
		consumersToCancel
				.forEach(consumer -> {
					try {
						RabbitUtils.closeMessageConsumer(consumer.getChannel(),
								Collections.singletonList(consumer.getConsumerTag()), isChannelTransacted());
					}
					catch (Exception e) {
						if (logger.isDebugEnabled()) {
							logger.debug("Error closing consumer " + consumer, e);
						}
					}
					this.logger.error("Consumer canceled - channel closed " + consumer);
					consumer.cancelConsumer("Consumer " + consumer + " channel closed");
				});
	}

	private boolean restartConsumer(final Map<String, Queue> namesToQueues, List<SimpleConsumer> restartableConsumers,
			SimpleConsumer consumerArg) {

		SimpleConsumer consumer = consumerArg;
		Queue queue = namesToQueues.get(consumer.getQueue());
		if (queue != null && !StringUtils.hasText(queue.getName())) {
			// check to see if a broker-declared queue name has changed
			String actualName = queue.getActualName();
			if (StringUtils.hasText(actualName)) {
				namesToQueues.remove(consumer.getQueue());
				namesToQueues.put(actualName, queue);
				consumer = new SimpleConsumer(null, null, actualName, consumer.getIndex());
			}
		}
		try {
			doConsumeFromQueue(consumer.getQueue(), consumer.getIndex());
			return true;
		}
		catch (AmqpConnectException | AmqpIOException e) {
			this.logger.error("Cannot connect to server", e);
			if (e.getCause() instanceof AmqpApplicationContextClosedException) {
				this.logger.error("Application context is closed, terminating");
				this.taskScheduler.schedule(this::stop, Instant.now());
			}
			this.consumersToRestart.addAll(restartableConsumers);
			if (this.logger.isTraceEnabled()) {
				this.logger.trace("After restart exception, consumers to restart now: "
						+ this.consumersToRestart);
			}
			return false;
		}
	}

	private void startConsumers(final String[] queueNames) {
		if (isChangeConsumerThreadName()) {
			Thread.currentThread().setName(getThreadNameSupplier().apply(this));
		}
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
							this.taskScheduler.schedule(this::stop, Instant.now());
							break;
						}
						this.logger.error("Error creating consumer; retrying in " + nextBackOff, e);
						doShutdown();
						try {
							Thread.sleep(nextBackOff); // NOSONAR
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
	}

	protected void doRedeclareElementsIfNecessary() {
		String routingLookupKey = getRoutingLookupKey();
		if (routingLookupKey != null) {
			SimpleResourceHolder.push(getRoutingConnectionFactory(), routingLookupKey); // NOSONAR both never null here
		}
		try {
			redeclareElementsIfNecessary();
		}
		catch (Exception e) {
			this.logger.error("Failed to redeclare elements", e);
		}
		finally {
			if (routingLookupKey != null) {
				SimpleResourceHolder.pop(getRoutingConnectionFactory()); // NOSONAR never null here
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
					Class<?> clazz =
							ClassUtils.forName("org.springframework.amqp.rabbit.core.RabbitAdmin",
									ClassUtils.getDefaultClassLoader());

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
				doConsumeFromQueue(queue, i);
			}
		}
	}

	private void doConsumeFromQueue(String queue, int index) {
		if (!isActive()) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Consume from queue " + queue + " ignore, container stopping");
			}
			return;
		}
		String routingLookupKey = getRoutingLookupKey();
		if (routingLookupKey != null) {
			SimpleResourceHolder.push(getRoutingConnectionFactory(), routingLookupKey); // NOSONAR both never null here
		}
		Connection connection = null; // NOSONAR (close)
		try {
			connection = getConnectionFactory().createConnection();
		}
		catch (Exception e) {
			publishConsumerFailedEvent(e.getMessage(), false, e);
			addConsumerToRestart(new SimpleConsumer(null, null, queue, index));
			throw e instanceof AmqpConnectException // NOSONAR exception type check
					? (AmqpConnectException) e
					: new AmqpConnectException(e);
		}
		finally {
			if (routingLookupKey != null) {
				SimpleResourceHolder.pop(getRoutingConnectionFactory()); // NOSONAR never null here
			}
		}
		SimpleConsumer consumer = consume(queue, index, connection);
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

	@Nullable
	private SimpleConsumer consume(String queue, int index, Connection connection) {
		Channel channel = null;
		SimpleConsumer consumer = null;
		try {
			if (getConsumeDelay() > 0) {
				try {
					Thread.sleep(getConsumeDelay());
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			channel = connection.createChannel(isChannelTransacted());
			channel.basicQos(getPrefetchCount(), isGlobalQos());
			consumer = new SimpleConsumer(connection, channel, queue, index);
			channel.queueDeclarePassive(queue);
			consumer.consumerTag = channel.basicConsume(queue, getAcknowledgeMode().isAutoAck(),
					(getConsumerTagStrategy() != null
							? getConsumerTagStrategy().createConsumerTag(queue) : ""), // NOSONAR never null
					isNoLocal(), isExclusive(), getConsumerArguments(), consumer);
		}
		catch (AmqpApplicationContextClosedException e) {
			throw new AmqpConnectException(e);
		}
		catch (Exception e) {
			RabbitUtils.closeChannel(channel);
			RabbitUtils.closeConnection(connection);

			consumer = handleConsumeException(queue, index, consumer, e);
		}
		return consumer;
	}

	@Nullable
	private SimpleConsumer handleConsumeException(String queue, int index, @Nullable SimpleConsumer consumerArg,
			Exception e) {

		SimpleConsumer consumer = consumerArg;
		if (e.getCause() instanceof ShutdownSignalException
				&& e.getCause().getMessage().contains("in exclusive use")) {
			getExclusiveConsumerExceptionLogger().log(logger,
					"Exclusive consumer failure", e.getCause());
			publishConsumerFailedEvent("Consumer raised exception, attempting restart", false, e);
		}
		else if (e.getCause() instanceof ShutdownSignalException
				&& RabbitUtils.isPassiveDeclarationChannelClose((ShutdownSignalException) e.getCause())) {
			publishMissingQueueEvent(queue);
			this.logger.error("Queue not present, scheduling consumer "
					+ (consumer == null ? "for queue " + queue : consumer) + " for restart", e);
		}
		else if (this.logger.isWarnEnabled()) {
			this.logger.warn("basicConsume failed, scheduling consumer "
					+ (consumer == null ? "for queue " + queue : consumer) + " for restart", e);
		}

		if (consumer == null) {
			addConsumerToRestart(new SimpleConsumer(null, null, queue, index));
		}
		else {
			addConsumerToRestart(consumer);
			consumer = null;
		}
		return consumer;
	}

	@Override
	protected void shutdownAndWaitOrCallback(@Nullable Runnable callback) {
		LinkedList<SimpleConsumer> canceledConsumers = null;
		boolean waitForConsumers = false;
		synchronized (this.consumersMonitor) {
			if (this.started || this.aborted) {
				// Copy in the same order to avoid ConcurrentModificationException during remove in the
				// cancelConsumer().
				canceledConsumers = new LinkedList<>(this.consumers);
				actualShutDown(canceledConsumers);
				waitForConsumers = true;
			}
		}
		if (waitForConsumers) {
			LinkedList<SimpleConsumer> consumersToWait = canceledConsumers;
			Runnable awaitShutdown = () -> {
				try {
					if (this.cancellationLock.await(getShutdownTimeout(), TimeUnit.MILLISECONDS)) {
						this.logger.info("Successfully waited for consumers to finish.");
					}
					else {
						this.logger.info("Consumers not finished.");
						if (isForceCloseChannel() || this.stopNow.get()) {
							consumersToWait.forEach(consumer -> {
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
				this.stopNow.set(false);
				runCallbackIfNotNull(callback);
			};
			if (callback == null) {
				awaitShutdown.run();
			}
			else {
				getTaskExecutor().execute(awaitShutdown);
			}
		}
	}

	private void runCallbackIfNotNull(@Nullable Runnable callback) {
		if (callback != null) {
			callback.run();
		}
	}

	/**
	 * Must hold this.consumersMonitor.
	 * @param consumers a copy of this.consumers.
	 */
	private void actualShutDown(List<SimpleConsumer> consumers) {
		Assert.state(getTaskExecutor() != null, "Cannot shut down if not initialized");
		this.logger.debug("Shutting down");
		if (isForceStop()) {
			this.stopNow.set(true);
		}
		else {
			consumers.forEach(this::cancelConsumer);
		}
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
				consumer.setCanceled(true);
				if (this.messagesPerAck > 1) {
					try {
						consumer.ackIfNecessary(0L);
					}
					catch (Exception e) {
						this.logger.error("Exception while sending delayed ack", e);
					}
				}
			}
			RabbitUtils.cancel(consumer.getChannel(), consumer.getConsumerTag());
		}
		finally {
			this.consumers.remove(consumer);
			consumerRemoved(consumer);
		}
	}

	private void addConsumerToRestart(SimpleConsumer consumer) {
		this.consumersToRestart.add(consumer);
		if (this.logger.isTraceEnabled()) {
			this.logger.trace("Consumers to restart now: " + this.consumersToRestart);
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

		private final int index;

		private final boolean ackRequired;

		private final ConnectionFactory connectionFactory = getConnectionFactory();

		private final PlatformTransactionManager transactionManager = getTransactionManager();

		private final TransactionAttribute transactionAttribute = getTransactionAttribute();

		private final boolean isRabbitTxManager = this.transactionManager instanceof RabbitTransactionManager;

		private final int messagesPerAck = DirectMessageListenerContainer.this.messagesPerAck;

		private final long ackTimeout = DirectMessageListenerContainer.this.ackTimeout;

		private final Channel targetChannel;

		private int pendingAcks;

		private long lastAck = System.currentTimeMillis();

		private long latestDeferredDeliveryTag;

		private volatile String consumerTag;

		private volatile int epoch;

		private volatile TransactionTemplate transactionTemplate;

		private volatile boolean canceled;

		private volatile boolean ackFailed;

		SimpleConsumer(@Nullable Connection connection, @Nullable Channel channel, String queue, int index) {
			super(channel);
			this.connection = connection;
			this.queue = queue;
			this.index = index;
			this.ackRequired = !getAcknowledgeMode().isAutoAck() && !getAcknowledgeMode().isManual();
			if (channel instanceof ChannelProxy proxy) {
				this.targetChannel = proxy.getTargetChannel();
			}
			else {
				this.targetChannel = null;
			}
		}

		String getQueue() {
			return this.queue;
		}

		int getIndex() {
			return this.index;
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
		 * Set to true to indicate this consumer is canceled and should send any pending
		 * acks.
		 * @param canceled the canceled to set
		 */
		void setCanceled(boolean canceled) {
			this.canceled = canceled;
		}

		/**
		 * True if an ack/nack failed (probably due to a closed channel).
		 * @return the ackFailed
		 */
		boolean isAckFailed() {
			return this.ackFailed;
		}

		/**
		 * True if the channel is a proxy and the underlying channel has changed.
		 * @return true if the condition exists.
		 */
		boolean targetChanged() {
			return this.targetChannel != null
					&& !((ChannelProxy) getChannel()).getTargetChannel().equals(this.targetChannel);
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
				BasicProperties properties, byte[] body) {

			if (!getChannel().isOpen()) {
				this.logger.debug("Discarding prefetch, channel closed");
				return;
			}
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
			Object data = message;
			List<Message> debatched = debatch(message);
			if (debatched != null) {
				data = debatched;
			}
			if (this.transactionManager != null) {
				try {
					executeListenerInTransaction(data, deliveryTag);
				}
				catch (WrappedTransactionException ex) {
					if (ex.getCause() instanceof Error error) {
						throw error;
					}
				}
				catch (Exception e) {
					// empty
				}
				finally {
					if (this.isRabbitTxManager) {
						ConsumerChannelRegistry.unRegisterConsumerChannel();
					}
				}
			}
			else {
				try {
					callExecuteListener(data, deliveryTag);
				}
				catch (Exception e) {
					// NOSONAR
				}
			}
			if (DirectMessageListenerContainer.this.stopNow.get()) {
				closeChannel();
			}
		}

		private void executeListenerInTransaction(Object data, long deliveryTag) {
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
					callExecuteListener(data, deliveryTag);
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

		private void callExecuteListener(Object data, long deliveryTag) { // NOSONAR (complex)
			boolean channelLocallyTransacted = isChannelLocallyTransacted();
			try {
				executeListener(getChannel(), data);
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
							RabbitResourceHolder resourceHolder =
									(RabbitResourceHolder) TransactionSynchronizationManager
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
			catch (Error e) { // NOSONAR
				this.logger.error("Failed to invoke listener", e);
				getJavaLangErrorHandler().handle(e);
				throw e;
			}
		}

		private void handleAck(long deliveryTag, boolean channelLocallyTransacted) {
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
							this.latestDeferredDeliveryTag = deliveryTag;
							this.pendingAcks++;
							ackIfNecessary(this.lastAck);
						}
					}
					else if (!isChannelTransacted() || isLocallyTransacted) {
						sendAckWithNotify(deliveryTag, false);
					}
				}
				if (isLocallyTransacted) {
					RabbitUtils.commitIfNecessary(getChannel());
				}
			}
			catch (Exception e) {
				this.ackFailed = true;
				this.logger.error("Error acking", e);
			}
		}

		/**
		 * Send the ack if we've reached the threshold (count or time) or immediately if
		 * we are processing deliveries after a cancel has been issued.
		 * @param now the current time.
		 * @throws IOException if one occurs.
		 */
		synchronized void ackIfNecessary(long now) throws Exception { // NOSONAR
			if (this.pendingAcks >= this.messagesPerAck || (
					this.pendingAcks > 0 && (now - this.lastAck > this.ackTimeout || this.canceled))) {
				sendAck(now);
			}
		}

		private void rollback(long deliveryTag, Exception e) {
			if (isChannelTransacted()) {
				RabbitUtils.rollbackIfNecessary(getChannel());
			}
			if (this.ackRequired || ContainerUtils.isRejectManual(e)) {
				try {
					if (this.messagesPerAck > 1) {
						synchronized (this) {
							if (this.pendingAcks > 0) {
								sendAck(System.currentTimeMillis());
							}
						}
					}
					getChannel().basicNack(deliveryTag, !isAsyncReplies(),
							ContainerUtils.shouldRequeue(isDefaultRequeueRejected(), e, this.logger));
				}
				catch (Exception e1) {
					this.logger.error("Failed to nack message", e1);
				}
			}
			if (isChannelTransacted()) {
				RabbitUtils.commitIfNecessary(getChannel());
			}
		}

		protected synchronized void sendAck(long now) throws Exception { // NOSONAR
			sendAckWithNotify(this.latestDeferredDeliveryTag, true);
			this.lastAck = now;
			this.pendingAcks = 0;
		}

		/**
		 * Send ack and notify MessageAckListener(if set).
		 * @param deliveryTag DeliveryTag of this ack.
		 * @param multiple Whether multiple ack.
		 * @throws Exception Occured when ack.
		 * @Since 2.4.6
		 */
		private void sendAckWithNotify(long deliveryTag, boolean multiple) throws Exception { // NOSONAR
			try {
				getChannel().basicAck(deliveryTag, multiple);
				notifyMessageAckListener(true, deliveryTag, null);
			}
			catch (Exception e) {
				notifyMessageAckListener(false, deliveryTag, e);
				throw e;
			}
		}

		/**
		 * Notify MessageAckListener set on message listener.
		 * @param messageAckListener MessageAckListener set on the message listener.
		 * @param success Whether ack succeeded.
		 * @param deliveryTag The deliveryTag of ack.
		 * @param cause If an exception occurs.
		 * @since 2.4.6
		 */
		private void notifyMessageAckListener(boolean success, long deliveryTag, @Nullable Throwable cause) {
			try {
				getMessageAckListener().onComplete(success, deliveryTag, cause);
			}
			catch (Exception e) {
				this.logger.error("An exception occured on MessageAckListener.", e);
			}
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
		public void handleCancel(String consumerTag) {
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
			closeChannel();
			consumerRemoved(this);
		}

		private void closeChannel() {
			RabbitUtils.setPhysicalCloseRequired(getChannel(), true);
			RabbitUtils.closeChannel(getChannel());
			RabbitUtils.closeConnection(this.connection);
			DirectMessageListenerContainer.this.cancellationLock.release(this);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getEnclosingInstance().hashCode();
			result = prime * result + this.index;
			result = prime * result + ((this.queue == null) ? 0 : this.queue.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			SimpleConsumer other = (SimpleConsumer) obj;
			if (!getEnclosingInstance().equals(other.getEnclosingInstance())) {
				return false;
			}
			if (this.index != other.index) {
				return false;
			}
			if (this.queue == null) {
				if (other.queue != null) {
					return false;
				}
			}
			else if (!this.queue.equals(other.queue)) {
				return false;
			}
			return true;
		}

		private DirectMessageListenerContainer getEnclosingInstance() {
			return DirectMessageListenerContainer.this;
		}

		@Override
		public String toString() {
			return "SimpleConsumer [queue=" + this.queue + ", index=" + this.index
					+ ", consumerTag=" + this.consumerTag
					+ " identity=" + ObjectUtils.getIdentityHexString(this) + "]";
		}

	}

}
