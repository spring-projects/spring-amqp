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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;

import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
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

	private volatile TaskExecutor taskExecutor;

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
	 * Set a task executor for the container - used to create the consumers not at
	 * runtime.
	 * @param taskExecutor the task executor.
	 */
	public void setTaskExecutor(TaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
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
		super.doStart();
		if (this.taskExecutor == null) {
			this.taskExecutor = new SimpleAsyncTaskExecutor(
					(getBeanName() == null ? "container" : getBeanName()) + "-");
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
		AtomicBoolean initialized = new AtomicBoolean();
		this.taskExecutor.execute(() -> {

			synchronized (this.consumersMonitor) {
				while (!initialized.get() && isRunning()) {
					this.cancellationLock.reset();
					try {
						for (String queue : getQueueNames()) {
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
					initialized.set(true);
				}
			}

		});
		if (logger.isInfoEnabled()) {
			this.logger.info("Container initialized for queues: " + Arrays.asList(getQueueNames()));
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
		RabbitUtils.setPhysicalCloseRequired(true);
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
		Assert.state(this.taskExecutor != null, "Cannot shut down if not initialized");
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
	}

	private final class SimpleConsumer extends DefaultConsumer {

		private final Log logger = DirectMessageListenerContainer.this.logger;

		private final String queue;

		private final boolean ackRequired;

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
			if (this.logger.isDebugEnabled()) {
				this.logger.debug(this + " received " + message);
			}
			try {
				if (!isRunning()) {
					if (this.logger.isWarnEnabled()) {
						this.logger.warn("Rejecting received message because the listener container has been stopped: "
								+ message);
					}
					throw new MessageRejectedWhileStoppingException();
				}
				invokeListener(getChannel(), message);
				if (this.ackRequired) {
					getChannel().basicAck(envelope.getDeliveryTag(), false);
				}
			}
			catch (Exception e) {
				this.logger.error("Failed to invoke listener", e);
				boolean shouldRequeue = isDefaultRequeueRejected() ||
						e instanceof MessageRejectedWhileStoppingException;
				Throwable t = e;
				while (shouldRequeue && t != null) {
					if (t instanceof AmqpRejectAndDontRequeueException) {
						shouldRequeue = false;
					}
					t = t.getCause();
				}
				if (this.ackRequired) {
					getChannel().basicNack(envelope.getDeliveryTag(), false, shouldRequeue);
				}
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
			RabbitUtils.closeChannel(getChannel());
		}

		@Override
		public String toString() {
			return "SimpleConsumer [queue=" + this.queue + ", consumerTag=" + this.getConsumerTag() + "]";
		}

	}

}
