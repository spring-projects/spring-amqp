/*
 * Copyright 2002-2010 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.listener;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.listener.BlockingQueueConsumer.Delivery;
import org.springframework.amqp.rabbit.listener.adapter.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 */
public class SimpleMessageListenerContainer extends AbstractMessageListenerContainer {

	public static final long DEFAULT_RECEIVE_TIMEOUT = 1000;

	private static final int DEFAULT_PREFETCH_COUNT = 10;

	private volatile int prefetchCount = DEFAULT_PREFETCH_COUNT;

	private volatile int txSize = 1;

	private volatile Executor taskExecutor = new SimpleAsyncTaskExecutor();

	private volatile int concurrentConsumers = 1;

	private final Semaphore cancellationLock = new Semaphore(0);

	private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

	// implied unlimited capacity
	private volatile int blockingQueueConsumerCapacity = -1;

	private volatile Set<Channel> channels = null;

	private volatile Set<BlockingQueueConsumer> consumers;

	private final Object consumersMonitor = new Object();

	private PlatformTransactionManager transactionManager;

	private DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();

	public SimpleMessageListenerContainer() {
	}

	public SimpleMessageListenerContainer(ConnectionFactory connectionFactory) {
		this.setConnectionFactory(connectionFactory);
	}

	/**
	 * Specify the number of concurrent consumers to create. Default is 1.
	 * <p>
	 * Raising the number of concurrent consumers is recommended in order to scale the consumption of messages coming in
	 * from a queue. However, note that any ordering guarantees are lost once multiple consumers are registered. In
	 * general, stick with 1 consumer for low-volume queues.
	 */
	public void setConcurrentConsumers(int concurrentConsumers) {
		Assert.isTrue(concurrentConsumers > 0, "'concurrentConsumers' value must be at least 1 (one)");
		this.concurrentConsumers = concurrentConsumers;
	}

	public void setReceiveTimeout(long receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
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

	public int getTxSize() {
		return txSize;
	}

	public void setTxSize(int txSize) {
		this.txSize = txSize;
	}

	public int getBlockingQueueConsumerCapacity() {
		return blockingQueueConsumerCapacity;
	}

	public void setBlockingQueueConsumerCapacity(int blockingQueueConsumerCapacity) {
		this.blockingQueueConsumerCapacity = blockingQueueConsumerCapacity;
	}

	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	/**
	 * Avoid the possibility of not configuring the CachingConnectionFactory in sync with the number of concurrent
	 * consumers.
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
	 * Creates the specified number of concurrent consumers, in the form of a Rabbit Channel plus associated
	 * MessageConsumer.
	 * 
	 * @throws Exception
	 */
	protected void doInitialize() throws Exception {
		establishSharedConnection();
		// initializeConsumers();
	}

	/**
	 * Re-initializes this container's Rabbit message consumers, if not initialized already. Then submits each consumer
	 * to this container's task executor.
	 * 
	 * @throws Exception
	 */
	protected void doStart() throws Exception {
		super.doStart();
		initializeConsumers();
		for (BlockingQueueConsumer consumer : this.consumers) {
			this.taskExecutor.execute(new AsyncMessageProcessingConsumer(consumer, this.txSize, this));
		}
	}

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
			cancellationLock.acquire(consumers.size());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		logger.debug("Closing Rabbit Consumers");
		synchronized (this.consumersMonitor) {
			logger.debug("Closing Rabbit Channels");
			try {
				for (Channel channel : this.channels) {
					RabbitUtils.closeChannel(channel);
				}
			} finally {
				cancellationLock.release(consumers.size());
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
				for (int i = 0; i < this.concurrentConsumers; i++) {
					Channel channel = getTransactionalResourceHolder().getChannel();
					BlockingQueueConsumer consumer = createBlockingQueueConsumer(channel);
					this.channels.add(channel);
					this.consumers.add(consumer);
				}
				cancellationLock.release(consumers.size());
			}
		}
	}

	protected boolean isChannelLocallyTransacted(Channel channel) {
		return super.isChannelLocallyTransacted(channel) && this.transactionManager == null;
	}

	protected BlockingQueueConsumer createBlockingQueueConsumer(final Channel channel) throws IOException {
		BlockingQueueConsumer consumer;
		if (this.blockingQueueConsumerCapacity <= 0) {
			consumer = new BlockingQueueConsumer(channel);
		} else {
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

	private class AsyncMessageProcessingConsumer implements Runnable {

		private BlockingQueueConsumer queue;

		private int txSize;

		private SimpleMessageListenerContainer messageListenerContainer;

		private PlatformTransactionManager transactionManager;

		private DefaultTransactionDefinition transactionDefinition;

		private long receiveTimeout;

		public AsyncMessageProcessingConsumer(BlockingQueueConsumer q, int txSize,
				SimpleMessageListenerContainer messageListenerContainer) {
			this.queue = q;
			this.txSize = txSize;
			this.messageListenerContainer = messageListenerContainer;
			this.transactionManager = messageListenerContainer.transactionManager;
			this.transactionDefinition = messageListenerContainer.transactionDefinition;
			this.receiveTimeout = messageListenerContainer.receiveTimeout;
		}

		public void run() {

			try {
				cancellationLock.acquire();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			try {
				while (isRunning()) {
					try {
						if (this.transactionManager != null) {
							// Execute within transaction.
							transactionalReceiveAndExecute();
						} else {
							receiveAndExecute();
						}
					} catch (ListenerExecutionFailedException ex) {
						// Continue to process, otherwise re-throw
					}
				}
			} catch (InterruptedException e) {
				logger.debug("Consumer thread interrupted, processing stopped.");
				Thread.currentThread().interrupt();
			} catch (ShutdownSignalException e) {
				logger.debug("Consumer received ShutdownSignal, processing stopped.");
			} catch (Throwable t) {
				logger.debug("Consumer received fatal exception, processing stopped.", t);
			} finally {
				Channel channel = queue.getChannel();
				logger.info("Closing consumer on channel: " + channel);
				RabbitUtils.closeMessageConsumer(channel, queue.getConsumerTag(), isChannelTransacted());
				cancellationLock.release();
			}
		}

		private void transactionalReceiveAndExecute() throws Exception {
			try {
				new TransactionTemplate(this.transactionManager, this.transactionDefinition)
						.execute(new TransactionCallback<Void>() {
							public Void doInTransaction(TransactionStatus status) {
								try {
									receiveAndExecute();
								} catch (ListenerExecutionFailedException ex) {
									// These are expected
									throw ex;
								} catch (Exception ex) {
									throw new AmqpException("Unexpected exception on listener execution", ex);
								} catch (Error err) {
									throw err;
								} catch (Throwable t) {
									throw new AmqpException(t);
								}
								return null;
							}
						});
			} catch (Exception ex) {
				throw ex;
			} catch (Error err) {
				throw err;
			} catch (Throwable t) {
				throw new AmqpException(t);
			}
		}

		private boolean receiveAndExecute() throws Throwable {

			Channel channel = queue.getChannel();

			int totalMsgCount = 0;

			ConnectionFactory connectionFactory = messageListenerContainer.getConnectionFactory();
			ConnectionFactoryUtils
					.bindResourceToTransaction(new RabbitResourceHolder(channel), connectionFactory, true);

			for (int i = 0; i < txSize; i++) {
				logger.debug("Receiving message from consumer.");
				Delivery delivery = queue.nextDelivery(receiveTimeout);
				if (delivery == null) {
					return false;
				}

				byte[] body = delivery.getBody();
				Envelope envelope = delivery.getEnvelope();
				totalMsgCount++;

				if (logger.isDebugEnabled()) {
					logger.debug("Received message from exchange [" + envelope.getExchange() + "], routing-key ["
							+ envelope.getRoutingKey() + "]");
				}

				MessageProperties messageProperties = RabbitUtils.createMessageProperties(delivery.getProperties(),
						envelope, "UTF-8");
				messageProperties.setMessageCount(0);
				Message message = new Message(body, messageProperties);

				messageListenerContainer.executeListener(channel, message);

			}

			return true;

		}
	}

}
