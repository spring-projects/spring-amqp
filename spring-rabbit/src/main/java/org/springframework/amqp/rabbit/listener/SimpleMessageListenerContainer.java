/*
 * Copyright 2002-2010 the original author or authors.
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.BlockingQueueConsumer.Delivery;
import org.springframework.amqp.rabbit.listener.adapter.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 */
public class SimpleMessageListenerContainer extends AbstractMessageListenerContainer {

	private volatile int prefetchCount = 1;

	private volatile Executor taskExecutor = new SimpleAsyncTaskExecutor();

	private volatile int concurrentConsumers = 1;

	// implied unlimited capacity
	private volatile int blockingQueueConsumerCapacity = -1;

	private volatile Set<Channel> channels = null;

	private volatile Set<BlockingQueueConsumer> consumers;

	private final Object consumersMonitor = new Object();

	public SimpleMessageListenerContainer() {
	}

	public SimpleMessageListenerContainer(ConnectionFactory connectionFactory) {
		this.setConnectionFactory(connectionFactory);
	}

	/**
	 * Specify the number of concurrent consumers to create. Default is 1.
	 * <p>
	 * Raising the number of concurrent consumers is recommended in order to
	 * scale the consumption of messages coming in from a queue. However, note
	 * that any ordering guarantees are lost once multiple consumers are
	 * registered. In general, stick with 1 consumer for low-volume queues.
	 */
	public void setConcurrentConsumers(int concurrentConsumers) {
		Assert.isTrue(concurrentConsumers > 0, "'concurrentConsumers' value must be at least 1 (one)");
		this.concurrentConsumers = concurrentConsumers;
	}

	public void setTaskExecutor(Executor taskExecutor) {
		Assert.notNull(taskExecutor, "taskExecutor must not be null");
		this.taskExecutor = taskExecutor;
	}

	public int getPrefetchCount() {
		return prefetchCount;
	}

	public void setPrefetchCount(int prefetchCount) {
		this.prefetchCount = prefetchCount;
	}

	public int getBlockingQueueConsumerCapacity() {
		return blockingQueueConsumerCapacity;
	}

	public void setBlockingQueueConsumerCapacity(int blockingQueueConsumerCapacity) {
		this.blockingQueueConsumerCapacity = blockingQueueConsumerCapacity;
	}

	/**
	 * Avoid the possibility of not configuring the CachingConnectionFactory in
	 * sync with the number of concurrent consumers.
	 */
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		if (this.getConnectionFactory() instanceof CachingConnectionFactory) {
			CachingConnectionFactory cf = (CachingConnectionFactory) getConnectionFactory();
			if (cf.getChannelCacheSize() < this.concurrentConsumers) {
				throw new IllegalStateException(
						"CachingConnectionFactory's channelCacheSize can not be less than the number of concurrentConsumers");
			}
			// Default setting
			if (concurrentConsumers == 1) {
				// Set concurrent consumers to size of connection factory
				// channel cache.
				if (cf.getChannelCacheSize() > 1) {
					logger.info("Setting number of concurrent consumers to CachingConnectionFactory's ChannelCacheSize ["
							+ cf.getChannelCacheSize() + "]");
					this.concurrentConsumers = cf.getChannelCacheSize();
				}
			}
		}
	}

	// -------------------------------------------------------------------------
	// Implementation of AbstractMessageListenerContainer's template methods
	// -------------------------------------------------------------------------

	/**
	 * Always use a shared Rabbit Connection.
	 */
	protected final boolean sharedConnectionEnabled() {
		return true;
	}

	/**
	 * Creates the specified number of concurrent consumers, in the form of a
	 * Rabbit Channel plus associated MessageConsumer.
	 * 
	 * @throws Exception
	 */
	protected void doInitialize() throws Exception {
		establishSharedConnection();
		// initializeConsumers();
	}

	/**
	 * Re-initializes this container's Rabbit message consumers, if not
	 * initialized already. Then submits each consumer to this container's task
	 * executor.
	 * 
	 * @throws Exception
	 */
	protected void doStart() throws Exception {
		super.doStart();
		initializeConsumers();
		for (BlockingQueueConsumer consumer : this.consumers) {
			this.taskExecutor.execute(new AsyncMessageProcessingConsumer(consumer, 0, 0, this));
		}
	}

	protected void doStop() {
		doShutdown();
		super.doStop();
	}

	@Override
	protected void doShutdown() {
		if (!this.isRunning()) {
			return;
		}
		logger.debug("Closing Rabbit Consumers");
		synchronized (this.consumersMonitor) {
			for (BlockingQueueConsumer consumer : this.consumers) {
				RabbitUtils.closeMessageConsumer(consumer.getChannel(), consumer.getConsumerTag());
			}
			logger.debug("Closing Rabbit Channels");
			for (Channel channel : this.channels) {
				RabbitUtils.closeChannel(channel);
			}
			this.consumers = null;
			this.channels = null;
		}
	}

	protected void initializeConsumers() throws IOException {
		synchronized (this.consumersMonitor) {
			if (this.consumers == null) {
				this.channels = new HashSet<Channel>(this.concurrentConsumers);
				this.consumers = new HashSet<BlockingQueueConsumer>(this.concurrentConsumers);
				Connection con = getSharedConnection();
				for (int i = 0; i < this.concurrentConsumers; i++) {
					Channel channel = createChannel(con);
					if (this.isChannelLocallyTransacted(channel)) {
						channel.txSelect();
					}
					BlockingQueueConsumer consumer = createBlockingQueueConsumer(channel);
					this.channels.add(channel);
					this.consumers.add(consumer);
				}
			}
		}
	}

	protected BlockingQueueConsumer createBlockingQueueConsumer(final Channel channel) throws IOException {
		BlockingQueueConsumer consumer;
		if (this.blockingQueueConsumerCapacity <= 0) {
			consumer = new BlockingQueueConsumer(channel);
		}
		else {
			consumer = new BlockingQueueConsumer(channel, new LinkedBlockingQueue<Delivery>(
					blockingQueueConsumerCapacity));
		}
		// Set basicQos before calling basicConsume
		channel.basicQos(prefetchCount);
		String queueNames = getRequiredQueueName();
		String[] queue = StringUtils.commaDelimitedListToStringArray(queueNames);
		for (int i = 0; i < queue.length; i++) {
			channel.queueDeclarePassive(queue[i]);
			String consumerTag = channel.basicConsume(queue[i], !isChannelTransacted(), consumer);
			consumer.setConsumerTag(consumerTag);
		}
		return consumer;
	}

	protected void processMessage(Message message, Channel channel) {
		boolean exposeResource = isExposeListenerChannel();
		if (exposeResource) {
			TransactionSynchronizationManager.bindResource(getConnectionFactory(),
					new LocallyExposedRabbitResourceHolder(channel));
		}
		try {
			executeListener(channel, message);
		}
		finally {
			if (exposeResource) {
				TransactionSynchronizationManager.unbindResource(getConnectionFactory());
			}
		}
	}

	private class AsyncMessageProcessingConsumer implements Runnable {

		private BlockingQueueConsumer q;

		private int txSize;

		private long timeLimit;

		private SimpleMessageListenerContainer messageListenerContainer;

		public AsyncMessageProcessingConsumer(BlockingQueueConsumer q, int txSize, int timeLimit,
				SimpleMessageListenerContainer messageListenerContainer) {

			this.q = q;
			this.txSize = txSize;
			this.timeLimit = 1000L * timeLimit;
			this.messageListenerContainer = messageListenerContainer;
		}

		public void run() {
			long now;
			long startTime;
			startTime = now = System.currentTimeMillis();
			int totalMsgCount = 0;

			Channel channel = q.getChannel();
			try {
				for (; timeLimit == 0 || now < startTime + timeLimit; totalMsgCount++) {
					logger.debug("Receiving message from consumer.");
					Delivery delivery;
					if (timeLimit == 0) {
						delivery = q.nextDelivery();
					}
					else {
						delivery = q.nextDelivery(startTime + timeLimit - now);
						if (delivery == null)
							break;
					}
					byte[] body = delivery.getBody();
					Envelope envelope = delivery.getEnvelope();

					if (logger.isDebugEnabled()) {
						logger.debug("Received message from exchange [" + envelope.getExchange() + "], routing-key ["
								+ envelope.getRoutingKey() + "]");
					}

					MessageProperties messageProperties = RabbitUtils.createMessageProperties(delivery.getProperties(),
							envelope, "UTF-8");
					messageProperties.setMessageCount(0);
					Message message = new Message(body, messageProperties);

					try {
						messageListenerContainer.processMessage(message, channel);
					}
					catch (ListenerExecutionFailedException e) {
						logger.debug("Consumer failed.");
					}

					// TODO probably can't handle batching (txSize > 1) with
					// current implementation
					// TODO Need to call rollback
					if (txSize != 0 && totalMsgCount % txSize == 0) {
						channel.txCommit();
					}
					now = System.currentTimeMillis();
				}
				// TODO should we throw here?
			}
			// TODO: should only rethrow if it is an error from Rabbit, not the
			// listener
			catch (IOException e) {
				throw new AmqpException(e);
			}
			catch (InterruptedException e) {
				logger.debug("Consumer thread interrupted, processing stopped.");
				Thread.currentThread().interrupt();
			}
			catch (ShutdownSignalException e) {
				logger.debug("Consumer received ShutdownSignal, processing stopped.");
			}
		}
	}

}
