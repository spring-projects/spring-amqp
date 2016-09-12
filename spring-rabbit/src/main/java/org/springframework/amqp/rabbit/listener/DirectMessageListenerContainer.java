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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;

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
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

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

	private final List<SimpleConsumer> consumers = new LinkedList<>();

	private final MultiValueMap<String, SimpleConsumer> consumersByQueue = new LinkedMultiValueMap<>();

	private final MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private final ActiveObjectCounter<SimpleConsumer> cancellationLock = new ActiveObjectCounter<SimpleConsumer>();

	private TaskScheduler taskScheduler;

	private volatile boolean started;

	private volatile CountDownLatch startedLatch = new CountDownLatch(1);

	private volatile int consumersPerQueue = 1;

	private volatile long recoveryInterval = DEFAULT_RECOVERY_INTERVAL;

	private volatile ScheduledFuture<?> idleTask;

	private volatile long lastAlertAt;

	/**
	 * Create an instance with the provided connection factory.
	 * @param connectionFactory the connection factory.
	 */
	public DirectMessageListenerContainer(ConnectionFactory connectionFactory) {
		setConnectionFactory(connectionFactory);
	}

	/**
	 * Each queue runs in its own consumer; set this property to create multiple
	 * consumers for each queue.
	 * If the container is already running, the number of consumers per queue will
	 * be adjusted up or down as necessary.
	 * If the adjustment fails for any reason, you may have mismatched number of
	 * consumers on each queue. A subsequent (successful) call will fix the mismatch.
	 * @param consumersPerQueue the consumers per queue.
	 */
	public void setConsumersPerQueue(int consumersPerQueue) {
		Assert.isTrue(consumersPerQueue >= 1, "'consumersPerQueue' must be 1 or greater");
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
	 * Set the interval before retrying when starting the container; default 5000.
	 * @param recoveryInterval the interval.
	 */
	public void setRecoveryInterval(long recoveryInterval) {
		this.recoveryInterval = recoveryInterval;
	}

	@Override
	public void setQueueNames(String... queueName) {
		Assert.state(!isRunning(), "Cannot set queue names while running, use add/remove");
		super.setQueueNames(queueName);
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
				List<String> added = new ArrayList<>();
				queueNameStream.forEach(queue -> {
					if (current.contains(queue)) {
						this.logger.warn("Queue " + queue + " is already configured for this container: "
								+ this + ", ignoring add");
					}
					else {
						try {
							consumeFromQueue(queue);
							added.add(queue);
						}
						catch (IOException e) {
							this.logger.error("Failed to add queue: " + queue + " undoing adds (if any)");
							removeQueues(added.stream());
							throw new AmqpIOException(e);
						}
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
					.filter(consumer -> consumer != null)
					.flatMap(Collection::stream)
					.forEach(consumer -> {
							try {
								consumer.getChannel().basicCancel(consumer.getConsumerTag());
							}
							catch (IOException e) {
								this.logger.error("Failed to cancel consumer: " + consumer, e);
							}
						});
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
				for (String queue : getQueueNames()) {
					try {
						while (this.consumersByQueue.get(queue) == null
								|| this.consumersByQueue.get(queue).size() < newCount) {
							doConsumeFromQueue(queue);
						}
						List<SimpleConsumer> consumerList = this.consumersByQueue.get(queue);
						if (consumerList.size() > newCount) {
							int currentCount = consumerList.size();
							for (int i = newCount; i < currentCount; i++) {
								try {
									consumerList.get(i).getChannel().basicCancel(consumerList.get(i).getConsumerTag());
								}
								catch (IOException e) {
									this.logger.error("Failed to cancel consumer", e);
								}
							}
						}
					}
					catch (IOException e) {
						throw new AmqpIOException(
								"Failed to adjust the consumer count for '" + queue + "' aborting adjustment", e);
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
			threadPoolTaskScheduler.setThreadNamePrefix(getBeanName() + "-idleDetector-");
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
		super.doStart();
		if (getTaskExecutor() == null) {
			afterPropertiesSet();
		}
		long idleEventInterval = getIdleEventInterval();
		if (idleEventInterval > 0) {
			if (this.taskScheduler == null) {
				afterPropertiesSet();
			}
			this.idleTask = this.taskScheduler.scheduleAtFixedRate(() -> {
				long now = System.currentTimeMillis();
				if (now - getLastReceive() > idleEventInterval && now - this.lastAlertAt > idleEventInterval) {
					publishIdleContainerEvent(now - getLastReceive());
					this.lastAlertAt = now;
				}
			}, idleEventInterval / 2);
		}
		final String[] queueNames = getQueueNames();
		if (queueNames.length > 0) {
			getTaskExecutor().execute(() -> {

				synchronized (this.consumersMonitor) {
					while (!DirectMessageListenerContainer.this.started && isRunning()) {
						this.cancellationLock.reset();
						try {
							for (String queue : queueNames) {
								consumeFromQueue(queue);
							}
						}
						catch (Exception e) {
							this.logger.error("Error creating consumer; retrying in "
									+ DirectMessageListenerContainer.this.recoveryInterval, e);
							doShutdown();
							try {
								Thread.sleep(DirectMessageListenerContainer.this.recoveryInterval);
							}
							catch (InterruptedException e1) {
								Thread.currentThread().interrupt();
							}
							continue; // initialization failed; try again having rested for recovery-interval
						}
						DirectMessageListenerContainer.this.started = true;
						DirectMessageListenerContainer.this.startedLatch.countDown();
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

	private void consumeFromQueue(String queue) throws IOException {
		for (int i = 0; i < DirectMessageListenerContainer.this.consumersPerQueue; i++) {
			doConsumeFromQueue(queue);
		}
	}

	private SimpleConsumer doConsumeFromQueue(String queue) throws IOException {
		Connection connection = getConnectionFactory().createConnection();
		Channel channel = connection.createChannel(isChannelTransacted());
		channel.basicQos(getPrefetchCount());
		SimpleConsumer consumer = new SimpleConsumer(channel, queue);
		channel.basicConsume(queue, getAcknowledgeMode().isAutoAck(),
				(getConsumerTagStrategy() != null
						? getConsumerTagStrategy().createConsumerTag(queue) : ""),
				false, isExclusive(), getConsumerArguments(), consumer);
		this.cancellationLock.add(consumer);
		this.consumers.add(consumer);
		this.consumersByQueue.add(queue, consumer);
		return consumer;
	}

	@Override
	protected void doShutdown() {
		if (this.started) {
			actualShutDown();
		}
	}

	private void actualShutDown() {
		Assert.state(getTaskExecutor() != null, "Cannot shut down if not initialized");
		synchronized (this.consumersMonitor) {
			for (DefaultConsumer consumer : this.consumers) {
				try {
					consumer.getChannel().basicCancel(consumer.getConsumerTag());
				}
				catch (IOException e) {
					this.logger.error("Cancel Error", e);
				}
			}
			this.consumers.clear();
			this.consumersByQueue.clear();
		}
		if (this.idleTask != null) {
			this.idleTask.cancel(true);
			this.idleTask = null;
		}
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
		}
	}

	private final class SimpleConsumer extends DefaultConsumer {

		private final Log logger = DirectMessageListenerContainer.this.logger;

		private final String queue;

		private final boolean ackRequired;

		private final ConnectionFactory connectionFactory = getConnectionFactory();

		private final PlatformTransactionManager transactionManager = getTransactionManager();

		private final TransactionAttribute transactionAttribute = getTransactionAttribute();

		private final boolean isRabbitTxManager = this.transactionManager instanceof RabbitTransactionManager;

		private SimpleConsumer(Channel channel, String queue) {
			super(channel);
			this.queue = queue;
			this.ackRequired = !getAcknowledgeMode().isAutoAck() && !getAcknowledgeMode().isManual();
		}

		@Override
		public void handleDelivery(String consumerTag, Envelope envelope,
				BasicProperties properties, byte[] body) throws IOException {
			MessageProperties messageProperties = DirectMessageListenerContainer.this.messagePropertiesConverter
					.toMessageProperties(properties, envelope, "UTF-8");
			messageProperties.setConsumerTag(consumerTag);
			messageProperties.setConsumerQueue(this.queue);
			Message message = new Message(body, messageProperties);
			long deliveryTag = envelope.getDeliveryTag();
			if (this.logger.isDebugEnabled()) {
				this.logger.debug(this + " received " + message);
			}
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
				this.logger.debug("New " + this);
			}
		}

		@Override
		public void handleCancelOk(String consumerTag) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("CancelOk " + this);
			}
			removeConsumer();
		}

		@Override
		public void handleCancel(String consumerTag) throws IOException {
			this.logger.error("Consumer canceled - queue deleted? " + this);
			publishConsumerFailedEvent("Consumer " + this + " canceled", true, null);
			removeConsumer();
		}

		private void removeConsumer() {
			synchronized (DirectMessageListenerContainer.this.consumersMonitor) {
				List<SimpleConsumer> list = DirectMessageListenerContainer.this.consumersByQueue.get(this.queue);
				if (list != null) {
					list.remove(this);
				}
				DirectMessageListenerContainer.this.cancellationLock.release(this);
				DirectMessageListenerContainer.this.consumers.remove(this);
			}
			RabbitUtils.setPhysicalCloseRequired(true);
			RabbitUtils.closeChannel(getChannel());
		}

		@Override
		public String toString() {
			return "SimpleConsumer [queue=" + this.queue + ", consumerTag=" + this.getConsumerTag() + "]";
		}

	}

}
