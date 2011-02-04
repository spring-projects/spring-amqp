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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.listener.adapter.ListenerExecutionFailedException;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 */
public class SimpleMessageListenerContainer extends AbstractMessageListenerContainer {

	public static final long DEFAULT_RECEIVE_TIMEOUT = 1000;

	private static final int DEFAULT_PREFETCH_COUNT = 1;

	private static final long DEFAULT_SHUTDOWN_TIMEOUT = 5000;

	private volatile int prefetchCount = DEFAULT_PREFETCH_COUNT;

	private volatile int txSize = 1;

	private volatile Executor taskExecutor = new SimpleAsyncTaskExecutor();

	private volatile int concurrentConsumers = 1;

	private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

	private long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

	private volatile Set<BlockingQueueConsumer> consumers;

	private final Object consumersMonitor = new Object();

	private PlatformTransactionManager transactionManager;

	private DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();

	private CountDownLatch cancellationLock;

	public SimpleMessageListenerContainer() {
	}

	public SimpleMessageListenerContainer(ConnectionFactory connectionFactory) {
		this.setConnectionFactory(connectionFactory);
	}

	/**
	 * Specify the number of concurrent consumers to create. Default is 1. <p> Raising the number of concurrent
	 * consumers is recommended in order to scale the consumption of messages coming in from a queue. However, note that
	 * any ordering guarantees are lost once multiple consumers are registered. In general, stick with 1 consumer for
	 * low-volume queues.
	 */
	public void setConcurrentConsumers(int concurrentConsumers) {
		Assert.isTrue(concurrentConsumers > 0, "'concurrentConsumers' value must be at least 1 (one)");
		this.concurrentConsumers = concurrentConsumers;
	}

	public void setReceiveTimeout(long receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	/**
	 * The time to wait for workers in milliseconds after the container is stopped, and before the connection is forced
	 * closed. If any workers are active when the shutdown signal comes they will be allowed to finish processing as
	 * long as they can finish within this timeout. Otherwise the connection is closed and messages remain unacked (if
	 * the channel is transactional). Defaults to 5 seconds.
	 * 
	 * @param shutdownTimeout the shutdown timeout to set
	 */
	public void setShutdownTimeout(long shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}

	public void setTaskExecutor(Executor taskExecutor) {
		Assert.notNull(taskExecutor, "taskExecutor must not be null");
		this.taskExecutor = taskExecutor;
	}

	/**
	 * Tells the broker how many messages to send to each consumer in a single request. Often this can be set quite high
	 * to improve throughput. It should be greater than or equal to {@link #setTxSize(int) the transaction size}.
	 * 
	 * @param prefetchCount the prefetch count
	 */
	public void setPrefetchCount(int prefetchCount) {
		this.prefetchCount = prefetchCount;
	}

	/**
	 * Tells the container how many messages to process in a single transaction (if the channel is transactional). For
	 * best results it should be less than or equal to {@link #setPrefetchCount(int) the prefetch count}.
	 * 
	 * @param prefetchCount the prefetch count
	 */
	public void setTxSize(int txSize) {
		this.txSize = txSize;
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
		cancellationLock = new CountDownLatch(this.consumers.size());
		for (BlockingQueueConsumer consumer : this.consumers) {
			this.taskExecutor
					.execute(new AsyncMessageProcessingConsumer(consumer, this.txSize, this, cancellationLock));
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
			logger.debug("Waiting for workers to finish.");
			boolean finished = cancellationLock.await(shutdownTimeout, TimeUnit.MILLISECONDS);
			if (finished) {
				logger.debug("Successfully waited for workers to finish.");
			} else {
				logger.debug("Workers not finished.  Forcing connections to close.");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.debug("Interrupted waiting for workers.  Continuing with shutdown.");
		}

		this.consumers = null;

	}

	protected void initializeConsumers() throws IOException {
		synchronized (this.consumersMonitor) {
			if (this.consumers == null) {
				Collection<Channel> channels = new ArrayList<Channel>();
				for (int i = 0; i < this.concurrentConsumers; i++) {
					Channel channel = getTransactionalResourceHolder().getChannel();
					channels.add(channel);
				}
				this.consumers = new HashSet<BlockingQueueConsumer>(this.concurrentConsumers);
				for (Channel channel : channels) {
					BlockingQueueConsumer consumer = createBlockingQueueConsumer(channel);
					this.consumers.add(consumer);
				}
			}
		}
	}

	protected boolean isChannelLocallyTransacted(Channel channel) {
		return super.isChannelLocallyTransacted(channel) && this.transactionManager == null;
	}

	protected BlockingQueueConsumer createBlockingQueueConsumer(final Channel channel) throws IOException {
		BlockingQueueConsumer consumer;
		String queueNames = getRequiredQueueName();
		String[] queues = StringUtils.commaDelimitedListToStringArray(queueNames);
		consumer = new BlockingQueueConsumer(channel, isChannelTransacted(), prefetchCount, queues);
		return consumer;
	}

	private class AsyncMessageProcessingConsumer implements Runnable {

		private final BlockingQueueConsumer consumer;

		private int txSize;

		private final PlatformTransactionManager transactionManager;

		private final DefaultTransactionDefinition transactionDefinition;

		private long receiveTimeout;

		private final CountDownLatch latch;

		public AsyncMessageProcessingConsumer(BlockingQueueConsumer consumer, int txSize,
				SimpleMessageListenerContainer messageListenerContainer, CountDownLatch latch) {
			this.consumer = consumer;
			this.txSize = txSize;
			this.latch = latch;
			this.transactionManager = messageListenerContainer.transactionManager;
			this.transactionDefinition = messageListenerContainer.transactionDefinition;
			this.receiveTimeout = messageListenerContainer.receiveTimeout;
		}

		public void run() {

			try {

				consumer.start();

				// Always better to stop receiving as soon as possible if transactional
				boolean continuable = false;
				while (isActive() || continuable) {
					try {
						if (this.transactionManager != null) {
							// Execute within transaction.
							transactionalReceiveAndExecute();
						} else {
							// Will come back false when the queue is drained
							continuable = receiveAndExecute() && !isChannelTransacted();
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
				latch.countDown();
				if (!isActive()) {
					logger.debug("Cancelling " + consumer);
					consumer.stop();
				}
			}
		}

		private boolean transactionalReceiveAndExecute() throws Exception {
			try {
				return new TransactionTemplate(this.transactionManager, this.transactionDefinition)
						.execute(new TransactionCallback<Boolean>() {
							public Boolean doInTransaction(TransactionStatus status) {
								try {
									return receiveAndExecute();
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

			Channel channel = consumer.getChannel();

			int totalMsgCount = 0;

			ConnectionFactory connectionFactory = getConnectionFactory();
			ConnectionFactoryUtils
					.bindResourceToTransaction(new RabbitResourceHolder(channel), connectionFactory, true);

			for (int i = 0; i < txSize; i++) {

				logger.debug("Waiting for message from consumer.");
				Message message = consumer.nextMessage(receiveTimeout);
				if (message == null) {
					return false;
				}
				totalMsgCount++;
				executeListener(channel, message);

			}

			return true;

		}
	}

}
