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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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

	private final List<SimpleConsumer> consumers = new ArrayList<SimpleConsumer>();

	private final MultiValueMap<String, SimpleConsumer> consumersByQueue = new LinkedMultiValueMap<>();

	private final MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private volatile TaskExecutor taskExecutor;

	private volatile int consumersPerQueue = 1;

	private volatile long recoveryInterval;

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
	 * @param consumersPerQueue the consumers per queue.
	 */
	public void setConsumersPerQueue(int consumersPerQueue) {
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

	@Override
	public void addQueueNames(String... queueNames) {
		addQueues(Arrays.asList(queueNames));
		super.addQueueNames(queueNames);
	}

	@Override
	public void addQueues(Queue... queues) {
		addQueues(Arrays.asList(queues)
				.stream()
				.map(Queue::getName)
				.collect(Collectors.toList()));
		super.addQueues(queues);
	}

	private void addQueues(Collection<String> queues) {
		if (isRunning()) {
			synchronized (this.consumersMonitor) {
				Set<String> current = getQueueNamesAsSet();
				for (String queue : queues) {
					if (current.contains(queue)) {
						throw new IllegalStateException("Queue " + queue + " is already configured for this container: "
								+ this + ", no queues added");
					}
				}
				List<String> added = new ArrayList<>();
				for (String queue : queues) {
					try {
						consumeFromQueue(queue);
						added.add(queue);
					}
					catch (IOException e) {
						logger.error("Failed to add queue: " + queue + " undoing adds (if any)");
						removeQueues(added);
						throw new AmqpIOException("Failed to add " + queues, e);
					}
				}
			}
		}
	}

	@Override
	public boolean removeQueueNames(String... queueNames) {
		removeQueues(Arrays.asList(queueNames));
		return super.removeQueueNames(queueNames);
	}

	@Override
	public boolean removeQueues(Queue... queues) {
		removeQueues(Arrays.asList(queues)
				.stream()
				.map(Queue::getName)
				.collect(Collectors.toList()));
		return super.removeQueues(queues);
	}

	private void removeQueues(Collection<String> queues) {
		if (isRunning()) {
			synchronized (this.consumersMonitor) {
				for (String queue : queues) {
					List<SimpleConsumer> consumersForQueue = this.consumersByQueue.remove(queue);
					if (consumersForQueue != null) {
						for (SimpleConsumer consumer : consumersForQueue) {
							try {
								consumer.getChannel().basicCancel(consumer.getConsumerTag());
								this.consumers.remove(consumer);
							}
							catch (IOException e) {
								logger.error("Failed to cancel consumer: " + consumer, e);
							}
						}
					}
				}
			}
		}
	}

	@Override
	protected boolean canRemoveLastQueue() {
		return true;
	}

	@Override
	protected void doInitialize() throws Exception {
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		if (this.taskExecutor == null) {
			this.taskExecutor = new SimpleAsyncTaskExecutor(
					(getBeanName() == null ? "container" : getBeanName()) + "-");
		}
		AtomicBoolean initialized = new AtomicBoolean();
		this.taskExecutor.execute(() -> {

			synchronized (this.consumersMonitor) {
				while (!initialized.get() && isRunning()) {
					try {
						for (String queue : getQueueNames()) {
							consumeFromQueue(queue);
						}
					}
					catch (Exception e) {
						logger.error("Error creating consumer; retrying in "
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
			logger.info("Container initialized for queues: " + Arrays.asList(getQueueNames()));
		}
	}

	private void consumeFromQueue(String queue) throws IOException {
		for (int i = 0; i < DirectMessageListenerContainer.this.consumersPerQueue; i++) {
			Connection connection = getConnectionFactory().createConnection();
			Channel channel = connection.createChannel(isChannelTransacted());
			channel.basicQos(getPrefetchCount());
			RabbitUtils.setPhysicalCloseRequired(true);
			SimpleConsumer consumer = new SimpleConsumer(channel, queue);
			channel.basicConsume(queue, getAcknowledgeMode().isAutoAck(),
					(getConsumerTagStrategy() != null
							? getConsumerTagStrategy().createConsumerTag(queue) : ""),
					false, isExclusive(), getConsumerArguments(), consumer);
			DirectMessageListenerContainer.this.consumers.add(consumer);
			DirectMessageListenerContainer.this.consumersByQueue.add(queue, consumer);
		}
	}

	@Override
	protected void doShutdown() {
		Assert.state(this.taskExecutor != null, "Cannot shut down if not initialized");
		for (DefaultConsumer consumer : this.consumers) {
			try {
				consumer.getChannel().basicCancel(consumer.getConsumerTag());
			}
			catch (IOException e) {
				logger.error("Cancel Error", e);
			}
		}
		this.consumers.clear();
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
		public void handleCancelOk(String consumerTag) {
			RabbitUtils.closeChannel(getChannel());
		}

		@Override
		public void handleCancel(String consumerTag) throws IOException {
			this.logger.error("Consumer canceled - queue deleted? " + this);
			List<SimpleConsumer> list = DirectMessageListenerContainer.this.consumersByQueue.get(this.queue);
			list.remove(this);
			DirectMessageListenerContainer.this.consumers.remove(this);
		}

		@Override
		public String toString() {
			return "SimpleConsumer [queue=" + this.queue + ", consumerTag=" + this.getConsumerTag() + "]";
		}

	}

}
