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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.aopalliance.aop.Advice;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.listener.adapter.ListenerExecutionFailedException;
import org.springframework.aop.Pointcut;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.MatchAlwaysTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Channel;

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

	private volatile int concurrentConsumers = 0;

	private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

	private long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

	private Set<BlockingQueueConsumer> consumers;

	private final Object consumersMonitor = new Object();

	private PlatformTransactionManager transactionManager;

	private TransactionAttribute transactionAttribute = new DefaultTransactionAttribute();

	private CountDownLatch cancellationLock;

	public static interface ContainerDelegate {
		boolean receiveAndExecute(BlockingQueueConsumer consumer) throws Throwable;
	}

	private Advice[] advices = new Advice[0];

	private ContainerDelegate delegate = new ContainerDelegate() {
		public boolean receiveAndExecute(BlockingQueueConsumer consumer) throws Throwable {
			return SimpleMessageListenerContainer.this.receiveAndExecute(consumer);
		}
	};

	private ContainerDelegate proxy = delegate;

	/**
	 * <p>
	 * Public setter for the {@link Advice} to apply to listener executions. If {@link #setTxSize(int) txSize>1} then
	 * multiple listener executions will all be wrapped in the same advice up to that limit.
	 * </p>
	 * <p>
	 * If a {@link #setTransactionManager(PlatformTransactionManager) transactionManager} is provided as well, then
	 * separate advice is created for the transaction and applied first in the chain. In that case the advice chain
	 * provided here should not contain a transaction interceptor (otherwise two transactions would be be applied).
	 * </p>
	 * 
	 * @param advices the advice chain to set
	 */
	public void setAdviceChain(Advice[] advices) {
		this.advices = advices;
	}

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
	 * @param transactionAttribute the transaction attribute to set
	 */
	public void setTransactionAttribute(TransactionAttribute transactionAttribute) {
		this.transactionAttribute = transactionAttribute;
	}

	/**
	 * Avoid the possibility of not configuring the CachingConnectionFactory in sync with the number of concurrent
	 * consumers.
	 */
	@Override
	protected void validateConfiguration() {

		super.validateConfiguration();

		Assert.state(
				!(getAcknowledgeMode().isAutoAck() && transactionManager != null),
				"The acknowledgeMode is NONE (autoack in Rabbit terms) which is not consistent with having an "
						+ "external transaction manager. Either use a different AcknowledgeMode or make sure the transactionManager is null.");

		if (this.getConnectionFactory() instanceof CachingConnectionFactory) {
			CachingConnectionFactory cf = (CachingConnectionFactory) getConnectionFactory();
			if (cf.getChannelCacheSize() < this.concurrentConsumers) {
				throw new IllegalStateException(
						"CachingConnectionFactory's channelCacheSize can not be less than the number of concurrentConsumers");
			}
			// Default setting
			if (concurrentConsumers < 1) {
				concurrentConsumers = 1;
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

	public void initializeProxy() {
		if (advices.length == 0 && transactionManager == null) {
			return;
		}
		ProxyFactory factory = new ProxyFactory();
		if (transactionManager != null) {
			MatchAlwaysTransactionAttributeSource txAttributeSource = new MatchAlwaysTransactionAttributeSource();
			txAttributeSource.setTransactionAttribute(transactionAttribute);
			Advice txAdvice = new TransactionInterceptor(transactionManager, txAttributeSource);
			factory.addAdvisor(new DefaultPointcutAdvisor(Pointcut.TRUE, txAdvice));
		}
		for (Advice advice : advices) {
			factory.addAdvisor(new DefaultPointcutAdvisor(Pointcut.TRUE, advice));
		}
		factory.setProxyTargetClass(false);
		factory.addInterface(ContainerDelegate.class);
		factory.setTarget(delegate);
		proxy = (ContainerDelegate) factory.getProxy();
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
		initializeProxy();
	}

	public int getActiveConsumerCount() {
		return (int) cancellationLock.getCount();
	}

	/**
	 * Re-initializes this container's Rabbit message consumers, if not initialized already. Then submits each consumer
	 * to this container's task executor.
	 * 
	 * @throws Exception
	 */
	protected void doStart() throws Exception {
		super.doStart();
		establishSharedConnection();
		initializeConsumers();
		synchronized (this.consumersMonitor) {
			if (this.consumers == null) {
				logger.info("Consumers were initialized and then cleared (presumably the container was stopped concurrently)");
				return;
			}
			cancellationLock = new CountDownLatch(this.consumers.size());
			for (BlockingQueueConsumer consumer : this.consumers) {
				this.taskExecutor.execute(new AsyncMessageProcessingConsumer(consumer, cancellationLock));
			}
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
				logger.info("Successfully waited for workers to finish.");
			} else {
				logger.info("Workers not finished.  Forcing connections to close.");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.warn("Interrupted waiting for workers.  Continuing with shutdown.");
		}

		synchronized (this.consumersMonitor) {
			this.consumers = null;
		}

	}

	protected void initializeConsumers() throws IOException {
		synchronized (this.consumersMonitor) {
			if (this.consumers == null) {
				this.consumers = new HashSet<BlockingQueueConsumer>(this.concurrentConsumers);
				for (int i = 0; i < this.concurrentConsumers; i++) {
					Channel channel = getTransactionalResourceHolder().getChannel();
					BlockingQueueConsumer consumer = createBlockingQueueConsumer(channel);
					this.consumers.add(consumer);
				}
			}
		}
	}

	protected boolean isChannelLocallyTransacted(Channel channel) {
		return super.isChannelLocallyTransacted(channel) && this.transactionManager == null;
	}

	protected BlockingQueueConsumer createBlockingQueueConsumer(final Channel channel) {
		BlockingQueueConsumer consumer;
		String queueNames = getRequiredQueueName();
		String[] queues = StringUtils.commaDelimitedListToStringArray(queueNames);
		consumer = new BlockingQueueConsumer(channel, getAcknowledgeMode(), isChannelTransacted(), prefetchCount,
				queues);
		return consumer;
	}

	private void restart(BlockingQueueConsumer consumer) {
		synchronized (this.consumersMonitor) {
			if (this.consumers != null) {
				try {
					// Need to recycle the channel in this consumer
					consumer.stop();
					this.consumers.remove(consumer);
					Channel channel = getTransactionalResourceHolder().getChannel();
					consumer = createBlockingQueueConsumer(channel);
					this.consumers.add(consumer);
				} catch (RuntimeException e) {
					// Ensure consumer counts are correct (another is not going to start because of the exception, but
					// we haven't counted down yet)
					logger.warn("Consumer died on restart. " + e.getClass() + ": " + e.getMessage());
					cancellationLock.countDown();
					// Thrown into the void (probably) in a background thread. Oh well, here goes...
					throw e;
				}
				this.taskExecutor.execute(new AsyncMessageProcessingConsumer(consumer, cancellationLock));
			}
		}
	}

	private boolean receiveAndExecute(BlockingQueueConsumer consumer) throws Throwable {

		Channel channel = consumer.getChannel();

		int totalMsgCount = 0;

		ConnectionFactory connectionFactory = getConnectionFactory();
		if (getAcknowledgeMode().isTransactionAllowed()) {
			ConnectionFactoryUtils
					.bindResourceToTransaction(new RabbitResourceHolder(channel), connectionFactory, true);
		}

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

	private class AsyncMessageProcessingConsumer implements Runnable {

		private final BlockingQueueConsumer consumer;

		private final CountDownLatch latch;

		public AsyncMessageProcessingConsumer(BlockingQueueConsumer consumer, CountDownLatch latch) {
			this.consumer = consumer;
			this.latch = latch;
		}

		public void run() {

			try {

				consumer.start();

				// Always better to stop receiving as soon as possible if transactional
				boolean continuable = false;
				while (isActive() || continuable) {
					try {
						// Will come back false when the queue is drained
						continuable = proxy.receiveAndExecute(consumer) && !isChannelTransacted();
					} catch (ListenerExecutionFailedException ex) {
						// Continue to process, otherwise re-throw
					}
				}

			} catch (InterruptedException e) {
				logger.debug("Consumer thread interrupted, processing stopped.");
				Thread.currentThread().interrupt();
			} catch (Throwable t) {
				logger.debug("Consumer received fatal exception, processing stopped.", t);
			} finally {
				if (!isActive()) {
					logger.debug("Cancelling " + consumer);
					latch.countDown();
					consumer.stop();
				} else {
					logger.debug("Restarting " + consumer);
					restart(consumer);
				}
			}

		}

	}

}
