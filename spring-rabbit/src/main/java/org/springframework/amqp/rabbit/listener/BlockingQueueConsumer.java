/*
 * Copyright 2002-2023 the original author or authors.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.ClosingRecoveryListener;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException;
import org.springframework.amqp.rabbit.listener.support.ContainerUtils;
import org.springframework.amqp.rabbit.support.ActiveObjectCounter;
import org.springframework.amqp.rabbit.support.ConsumerCancelledException;
import org.springframework.amqp.rabbit.support.Delivery;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.backoff.BackOffExecution;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.utility.Utility;

/**
 * Specialized consumer encapsulating knowledge of the broker
 * connections and having its own lifecycle (start and stop).
 *
 * @author Mark Pollack
 * @author Dave Syer
 * @author Gary Russell
 * @author Casper Mout
 * @author Artem Bilan
 * @author Alex Panchenko
 * @author Johno Crawford
 * @author Ian Roberts
 * @author Cao Weibo
 */
public class BlockingQueueConsumer {

	private static final int DEFAULT_DECLARATION_RETRIES = 3;

	private static final int DEFAULT_RETRY_DECLARATION_INTERVAL = 60000;

	private static Log logger = LogFactory.getLog(BlockingQueueConsumer.class);

	private final BlockingQueue<Delivery> queue;

	// When this is non-null the connection has been closed (should never happen in normal operation).
	private volatile ShutdownSignalException shutdown;

	private final String[] queues;

	private final int prefetchCount;

	private final boolean transactional;

	private Channel channel;

	private RabbitResourceHolder resourceHolder;

	private final ConcurrentMap<String, InternalConsumer> consumers = new ConcurrentHashMap<>();

	/**
	 * The flag indicating that consumer has been cancelled from all queues
	 * via {@code handleCancelOk} callback replies.
	 */
	private final AtomicBoolean cancelled = new AtomicBoolean(false);

	private final AcknowledgeMode acknowledgeMode;

	private final ConnectionFactory connectionFactory;

	private final MessagePropertiesConverter messagePropertiesConverter;

	private final ActiveObjectCounter<BlockingQueueConsumer> activeObjectCounter;

	private final Map<String, Object> consumerArgs = new HashMap<String, Object>();

	private final boolean noLocal;

	private final boolean exclusive;

	private final Set<Long> deliveryTags = new LinkedHashSet<Long>();

	private final boolean defaultRequeueRejected;

	private final Set<String> missingQueues = Collections.synchronizedSet(new HashSet<String>());

	private long retryDeclarationInterval = DEFAULT_RETRY_DECLARATION_INTERVAL;

	private long failedDeclarationRetryInterval =
			AbstractMessageListenerContainer.DEFAULT_FAILED_DECLARATION_RETRY_INTERVAL;

	private int declarationRetries = DEFAULT_DECLARATION_RETRIES;

	private long lastRetryDeclaration;

	private ConsumerTagStrategy tagStrategy;

	private BackOffExecution backOffExecution;

	private long shutdownTimeout;

	private boolean locallyTransacted;

	private ApplicationEventPublisher applicationEventPublisher;

	private long consumeDelay;

	private java.util.function.Consumer<String> missingQueuePublisher = str -> { };

	private boolean globalQos;

	private volatile long abortStarted;

	private volatile boolean normalCancel;

	volatile Thread thread; // NOSONAR package protected

	volatile boolean declaring; // NOSONAR package protected

	private MessageAckListener messageAckListener;

	/**
	 * Create a consumer. The consumer must not attempt to use
	 * the connection factory or communicate with the broker
	 * until it is started. RequeueRejected defaults to true.
	 * @param connectionFactory The connection factory.
	 * @param messagePropertiesConverter The properties converter.
	 * @param activeObjectCounter The active object counter; used during shutdown.
	 * @param acknowledgeMode The acknowledgemode.
	 * @param transactional Whether the channel is transactional.
	 * @param prefetchCount The prefetch count.
	 * @param queues The queues.
	 */
	public BlockingQueueConsumer(ConnectionFactory connectionFactory,
			MessagePropertiesConverter messagePropertiesConverter,
			ActiveObjectCounter<BlockingQueueConsumer> activeObjectCounter, AcknowledgeMode acknowledgeMode,
			boolean transactional, int prefetchCount, String... queues) {

		this(connectionFactory, messagePropertiesConverter, activeObjectCounter,
				acknowledgeMode, transactional, prefetchCount, true, queues);
	}

	/**
	 * Create a consumer. The consumer must not attempt to use
	 * the connection factory or communicate with the broker
	 * until it is started.
	 * @param connectionFactory The connection factory.
	 * @param messagePropertiesConverter The properties converter.
	 * @param activeObjectCounter The active object counter; used during shutdown.
	 * @param acknowledgeMode The acknowledge mode.
	 * @param transactional Whether the channel is transactional.
	 * @param prefetchCount The prefetch count.
	 * @param defaultRequeueRejected true to reject requeued messages.
	 * @param queues The queues.
	 */
	public BlockingQueueConsumer(ConnectionFactory connectionFactory,
			MessagePropertiesConverter messagePropertiesConverter,
			ActiveObjectCounter<BlockingQueueConsumer> activeObjectCounter, AcknowledgeMode acknowledgeMode,
			boolean transactional, int prefetchCount, boolean defaultRequeueRejected, String... queues) {
		this(connectionFactory, messagePropertiesConverter, activeObjectCounter, acknowledgeMode, transactional,
				prefetchCount, defaultRequeueRejected, null, queues);
	}

	/**
	 * Create a consumer. The consumer must not attempt to use the
	 * connection factory or communicate with the broker
	 * until it is started.
	 * @param connectionFactory The connection factory.
	 * @param messagePropertiesConverter The properties converter.
	 * @param activeObjectCounter The active object counter; used during shutdown.
	 * @param acknowledgeMode The acknowledge mode.
	 * @param transactional Whether the channel is transactional.
	 * @param prefetchCount The prefetch count.
	 * @param defaultRequeueRejected true to reject requeued messages.
	 * @param consumerArgs The consumer arguments (e.g. x-priority).
	 * @param queues The queues.
	 */
	public BlockingQueueConsumer(ConnectionFactory connectionFactory,
			MessagePropertiesConverter messagePropertiesConverter,
			ActiveObjectCounter<BlockingQueueConsumer> activeObjectCounter, AcknowledgeMode acknowledgeMode,
			boolean transactional, int prefetchCount, boolean defaultRequeueRejected,
			@Nullable Map<String, Object> consumerArgs, String... queues) {
		this(connectionFactory, messagePropertiesConverter, activeObjectCounter, acknowledgeMode, transactional,
				prefetchCount, defaultRequeueRejected, consumerArgs, false, queues);
	}

	/**
	 * Create a consumer. The consumer must not attempt to use
	 * the connection factory or communicate with the broker
	 * until it is started.
	 * @param connectionFactory The connection factory.
	 * @param messagePropertiesConverter The properties converter.
	 * @param activeObjectCounter The active object counter; used during shutdown.
	 * @param acknowledgeMode The acknowledge mode.
	 * @param transactional Whether the channel is transactional.
	 * @param prefetchCount The prefetch count.
	 * @param defaultRequeueRejected true to reject requeued messages.
	 * @param consumerArgs The consumer arguments (e.g. x-priority).
	 * @param exclusive true if the consumer is to be exclusive.
	 * @param queues The queues.
	 */
	public BlockingQueueConsumer(ConnectionFactory connectionFactory,
			MessagePropertiesConverter messagePropertiesConverter,
			ActiveObjectCounter<BlockingQueueConsumer> activeObjectCounter, AcknowledgeMode acknowledgeMode,
			boolean transactional, int prefetchCount, boolean defaultRequeueRejected,
			@Nullable Map<String, Object> consumerArgs, boolean exclusive, String... queues) {
		this(connectionFactory, messagePropertiesConverter, activeObjectCounter, acknowledgeMode, transactional,
				prefetchCount, defaultRequeueRejected, consumerArgs, false, exclusive, queues);
	}

	/**
	 * Create a consumer. The consumer must not attempt to use
	 * the connection factory or communicate with the broker
	 * until it is started.
	 * @param connectionFactory The connection factory.
	 * @param messagePropertiesConverter The properties converter.
	 * @param activeObjectCounter The active object counter; used during shutdown.
	 * @param acknowledgeMode The acknowledge mode.
	 * @param transactional Whether the channel is transactional.
	 * @param prefetchCount The prefetch count.
	 * @param defaultRequeueRejected true to reject requeued messages.
	 * @param consumerArgs The consumer arguments (e.g. x-priority).
	 * @param noLocal true if the consumer is to be no-local.
	 * @param exclusive true if the consumer is to be exclusive.
	 * @param queues The queues.
	 * @since 1.7.4
	 */
	public BlockingQueueConsumer(ConnectionFactory connectionFactory,
			MessagePropertiesConverter messagePropertiesConverter,
			ActiveObjectCounter<BlockingQueueConsumer> activeObjectCounter, AcknowledgeMode acknowledgeMode,
			boolean transactional, int prefetchCount, boolean defaultRequeueRejected,
			@Nullable Map<String, Object> consumerArgs, boolean noLocal, boolean exclusive, String... queues) {
		this.connectionFactory = connectionFactory;
		this.messagePropertiesConverter = messagePropertiesConverter;
		this.activeObjectCounter = activeObjectCounter;
		this.acknowledgeMode = acknowledgeMode;
		this.transactional = transactional;
		this.prefetchCount = prefetchCount;
		this.defaultRequeueRejected = defaultRequeueRejected;
		if (consumerArgs != null && consumerArgs.size() > 0) {
			this.consumerArgs.putAll(consumerArgs);
		}
		this.noLocal = noLocal;
		this.exclusive = exclusive;
		this.queues = Arrays.copyOf(queues, queues.length);
		this.queue = new LinkedBlockingQueue<Delivery>(queues.length == 0 ? prefetchCount : prefetchCount * queues.length);
	}

	public Channel getChannel() {
		return this.channel;
	}

	public Collection<String> getConsumerTags() {
		return this.consumers.values().stream()
				.map(c -> c.getConsumerTag())
				.filter(tag -> tag != null)
				.collect(Collectors.toList());
	}

	public void setShutdownTimeout(long shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}

	/**
	 * Set the number of retries after passive queue declaration fails.
	 * @param declarationRetries The number of retries, default 3.
	 * @since 1.3.9
	 * @see #setFailedDeclarationRetryInterval(long)
	 */
	public void setDeclarationRetries(int declarationRetries) {
		this.declarationRetries = declarationRetries;
	}

	/**
	 * Set the interval between passive queue declaration attempts in milliseconds.
	 * @param failedDeclarationRetryInterval the interval, default 5000.
	 * @since 1.3.9
	 * @see #setDeclarationRetries(int)
	 */
	public void setFailedDeclarationRetryInterval(long failedDeclarationRetryInterval) {
		this.failedDeclarationRetryInterval = failedDeclarationRetryInterval;
	}

	/**
	 * When consuming multiple queues, set the interval between declaration attempts when only
	 * a subset of the queues were available (milliseconds).
	 * @param retryDeclarationInterval the interval, default 60000.
	 * @since 1.3.9
	 */
	public void setRetryDeclarationInterval(long retryDeclarationInterval) {
		this.retryDeclarationInterval = retryDeclarationInterval;
	}

	/**
	 * Set the {@link ConsumerTagStrategy} to use when generating consumer tags.
	 * @param tagStrategy the tagStrategy to set
	 * @since 1.4.5
	 */
	public void setTagStrategy(ConsumerTagStrategy tagStrategy) {
		this.tagStrategy = tagStrategy;
	}

	/**
	 * Set the {@link BackOffExecution} to use for the recovery in the {@code SimpleMessageListenerContainer}.
	 * @param backOffExecution the backOffExecution.
	 * @since 1.5
	 */
	public void setBackOffExecution(BackOffExecution backOffExecution) {
		this.backOffExecution = backOffExecution;
	}

	public BackOffExecution getBackOffExecution() {
		return this.backOffExecution;
	}

	/**
	 * True if the channel is locally transacted.
	 * @param locallyTransacted the locally transacted to set.
	 * @since 1.6.6
	 */
	public void setLocallyTransacted(boolean locallyTransacted) {
		this.locallyTransacted = locallyTransacted;
	}

	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	/**
	 * Set the publisher for a missing queue event.
	 * @param missingQueuePublisher the publisher.
	 * @since 2.1.18
	 */
	public void setMissingQueuePublisher(java.util.function.Consumer<String> missingQueuePublisher) {
		this.missingQueuePublisher = missingQueuePublisher;
	}

	/**
	 * Set the consumeDelay - a time to wait before consuming in ms. This is useful when
	 * using the sharding plugin with {@code concurrency > 1}, to avoid uneven distribution of
	 * consumers across the shards. See the plugin README for more information.
	 * @param consumeDelay the consume delay.
	 * @since 2.3
	 */
	public void setConsumeDelay(long consumeDelay) {
		this.consumeDelay = consumeDelay;
	}

	/**
	 * Set a {@link MessageAckListener} to use when ack a message(messages) in
	 * {@link AcknowledgeMode#AUTO} mode.
	 * @param messageAckListener the messageAckListener.
	 * @since 2.4.6
	 */
	public void setMessageAckListener(MessageAckListener messageAckListener) {
		Assert.notNull(messageAckListener, "'messageAckListener' cannot be null");
		this.messageAckListener = messageAckListener;
	}

	/**
	 * Clear the delivery tags when rolling back with an external transaction
	 * manager.
	 * @since 1.6.6
	 */
	public void clearDeliveryTags() {
		this.deliveryTags.clear();
	}

	/**
	 * Apply prefetch to the entire channel.
	 * @param globalQos true for a channel-wide prefetch.
	 * @since 2.2.17
	 * @see Channel#basicQos(int, boolean)
	 */
	public void setGlobalQos(boolean globalQos) {
		this.globalQos = globalQos;
	}

	/**
	 * Return true if cancellation is expected.
	 * @return true if expected.
	 */
	public boolean isNormalCancel() {
		return this.normalCancel;
	}

	/**
	 * Return the size the queues array.
	 * @return the count.
	 */
	int getQueueCount() {
		return this.queues.length;
	}

	protected void basicCancel() {
		basicCancel(false);
	}

	protected void basicCancel(boolean expected) {
		this.normalCancel = expected;
		getConsumerTags().forEach(consumerTag -> {
			if (this.channel.isOpen()) {
				RabbitUtils.cancel(this.channel, consumerTag);
			}
		});
		this.cancelled.set(true);
		this.abortStarted = System.currentTimeMillis();
	}

	protected boolean hasDelivery() {
		return !this.queue.isEmpty();
	}

	protected boolean cancelled() {
		return this.cancelled.get() || (this.abortStarted > 0 &&
				this.abortStarted + this.shutdownTimeout > System.currentTimeMillis())
				|| !this.activeObjectCounter.isActive();
	}

	/**
	 * Check if we are in shutdown mode and if so throw an exception.
	 */
	private void checkShutdown() {
		if (this.shutdown != null) {
			throw Utility.fixStackTrace(this.shutdown);
		}
	}

	/**
	 * If this is a non-POISON non-null delivery simply return it.
	 * If this is POISON we are in shutdown mode, throw
	 * shutdown. If delivery is null, we may be in shutdown mode. Check and see.
	 * @param delivery the delivered message contents.
	 * @return A message built from the contents.
	 * @throws InterruptedException if the thread is interrupted.
	 */
	@Nullable
	private Message handle(@Nullable Delivery delivery) {
		if ((delivery == null && this.shutdown != null)) {
			throw this.shutdown;
		}
		if (delivery == null) {
			return null;
		}
		byte[] body = delivery.getBody();
		Envelope envelope = delivery.getEnvelope();

		MessageProperties messageProperties = this.messagePropertiesConverter.toMessageProperties(
				delivery.getProperties(), envelope, "UTF-8");
		messageProperties.setConsumerTag(delivery.getConsumerTag());
		messageProperties.setConsumerQueue(delivery.getQueue());
		Message message = new Message(body, messageProperties);
		if (logger.isDebugEnabled()) {
			logger.debug("Received message: " + message);
		}
		this.deliveryTags.add(messageProperties.getDeliveryTag());
		if (this.transactional && !this.locallyTransacted) {
			ConnectionFactoryUtils.registerDeliveryTag(this.connectionFactory, this.channel,
					delivery.getEnvelope().getDeliveryTag());
		}
		return message;
	}

	/**
	 * Main application-side API: wait for the next message delivery and return it.
	 * @return the next message
	 * @throws InterruptedException if an interrupt is received while waiting
	 * @throws ShutdownSignalException if the connection is shut down while waiting
	 */
	@Nullable
	public Message nextMessage() throws InterruptedException, ShutdownSignalException {
		if (logger.isTraceEnabled()) {
			logger.trace("Retrieving delivery for " + this);
		}
		return handle(this.queue.take());
	}

	/**
	 * Main application-side API: wait for the next message delivery and return it.
	 * @param timeout timeout in millisecond
	 * @return the next message or null if timed out
	 * @throws InterruptedException if an interrupt is received while waiting
	 * @throws ShutdownSignalException if the connection is shut down while waiting
	 */
	@Nullable
	public Message nextMessage(long timeout) throws InterruptedException, ShutdownSignalException {
		if (logger.isTraceEnabled()) {
			logger.trace("Retrieving delivery for " + this);
		}
		checkShutdown();
		if (this.missingQueues.size() > 0) {
			checkMissingQueues();
		}
		Message message = handle(this.queue.poll(timeout, TimeUnit.MILLISECONDS));
		if (message == null && this.cancelled.get()) {
			throw new ConsumerCancelledException();
		}
		return message;
	}

	/*
	 * Check to see if missing queues are now available; use a separate channel so the main
	 * channel is not closed by the broker if the declaration fails.
	 */
	private void checkMissingQueues() {
		long now = System.currentTimeMillis();
		if (now - this.retryDeclarationInterval > this.lastRetryDeclaration) {
			synchronized (this.missingQueues) {
				Iterator<String> iterator = this.missingQueues.iterator();
				while (iterator.hasNext()) {
					boolean available = true;
					String queueToCheck = iterator.next();
					Connection connection = null; // NOSONAR - RabbitUtils
					Channel channelForCheck = null;
					try {
						connection = this.connectionFactory.createConnection();
						channelForCheck = connection.createChannel(false);
						channelForCheck.queueDeclarePassive(queueToCheck);
						if (logger.isInfoEnabled()) {
							logger.info("Queue '" + queueToCheck + "' is now available");
						}
					}
					catch (IOException e) {
						available = false;
						if (logger.isWarnEnabled()) {
							logger.warn("Queue '" + queueToCheck + "' is still not available");
						}
					}
					finally {
						RabbitUtils.closeChannel(channelForCheck);
						RabbitUtils.closeConnection(connection);
					}
					if (available) {
						try {
							this.consumeFromQueue(queueToCheck);
							iterator.remove();
						}
						catch (IOException e) {
							throw RabbitExceptionTranslator.convertRabbitAccessException(e);
						}
					}
				}
			}
			this.lastRetryDeclaration = now;
		}
	}

	public void start() throws AmqpException {
		if (logger.isDebugEnabled()) {
			logger.debug("Starting consumer " + this);
		}

		this.thread = Thread.currentThread();

		try {
			this.resourceHolder = ConnectionFactoryUtils.getTransactionalResourceHolder(this.connectionFactory,
					this.transactional);
			this.channel = this.resourceHolder.getChannel();
			ClosingRecoveryListener.addRecoveryListenerIfNecessary(this.channel); // NOSONAR never null here
		}
		catch (AmqpAuthenticationException e) {
			throw new FatalListenerStartupException("Authentication failure", e);
		}
		this.deliveryTags.clear();
		this.activeObjectCounter.add(this);

		passiveDeclarations();
		setQosAndCreateConsumers();
	}

	private void passiveDeclarations() {
		// mirrored queue might be being moved
		int passiveDeclareRetries = this.declarationRetries;
		this.declaring = true;
		do {
			if (cancelled()) {
				break;
			}
			try {
				attemptPassiveDeclarations();
				if (passiveDeclareRetries < this.declarationRetries && logger.isInfoEnabled()) {
					logger.info("Queue declaration succeeded after retrying");
				}
				passiveDeclareRetries = 0;
			}
			catch (DeclarationException e) {
				handleDeclarationException(passiveDeclareRetries, e);
			}
		}
		while (passiveDeclareRetries-- > 0 && !cancelled());
		this.declaring = false;
	}

	private void setQosAndCreateConsumers() {
		if (this.consumeDelay > 0) {
			try {
				Thread.sleep(this.consumeDelay);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		if (!this.acknowledgeMode.isAutoAck() && !cancelled()) {
			try {
				this.channel.basicQos(this.prefetchCount, this.globalQos);
			}
			catch (IOException e) {
				this.activeObjectCounter.release(this);
				throw new AmqpIOException(e);
			}
		}

		try {
			if (!cancelled()) {
				for (String queueName : this.queues) {
					if (!this.missingQueues.contains(queueName)) {
						consumeFromQueue(queueName);
					}
				}
			}
		}
		catch (IOException e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	private void handleDeclarationException(int passiveDeclareRetries, DeclarationException e) {
		if (passiveDeclareRetries > 0 && this.channel.isOpen()) {
			if (logger.isWarnEnabled()) {
				logger.warn("Queue declaration failed; retries left=" + (passiveDeclareRetries), e);
			}
			try {
				Thread.sleep(this.failedDeclarationRetryInterval);
			}
			catch (InterruptedException e1) {
				this.declaring = false;
				Thread.currentThread().interrupt();
				this.activeObjectCounter.release(this);
				throw RabbitExceptionTranslator.convertRabbitAccessException(e1); // NOSONAR stack trace loss
			}
		}
		else if (e.getFailedQueues().size() < this.queues.length) {
			if (logger.isWarnEnabled()) {
				logger.warn("Not all queues are available; only listening on those that are - configured: "
						+ Arrays.asList(this.queues) + "; not available: " + e.getFailedQueues());
			}
			this.missingQueues.addAll(e.getFailedQueues());
			this.lastRetryDeclaration = System.currentTimeMillis();
		}
		else {
			this.declaring = false;
			this.activeObjectCounter.release(this);
			throw new QueuesNotAvailableException("Cannot prepare queue for listener. "
					+ "Either the queue doesn't exist or the broker will not allow us to use it.", e);
		}
	}

	private void consumeFromQueue(String queue) throws IOException {
		InternalConsumer consumer = new InternalConsumer(this.channel, queue);
		String consumerTag = this.channel.basicConsume(queue, this.acknowledgeMode.isAutoAck(),
				(this.tagStrategy != null ? this.tagStrategy.createConsumerTag(queue) : ""), this.noLocal,
				this.exclusive, this.consumerArgs,
				consumer);

		if (consumerTag != null) {
			this.consumers.put(queue, consumer);
			if (logger.isDebugEnabled()) {
				logger.debug("Started on queue '" + queue + "' with tag " + consumerTag + ": " + this);
			}
		}
		else {
			logger.error("Null consumer tag received for queue " + queue);
		}
	}

	private void attemptPassiveDeclarations() {
		DeclarationException failures = null;
		for (String queueName : this.queues) {
			try {
				try {
					this.channel.queueDeclarePassive(queueName);
				}
				catch (IllegalArgumentException e) {
					try {
						if (this.channel instanceof ChannelProxy proxy) {
							proxy.getTargetChannel().close();
						}
					}
					catch (TimeoutException e1) {
					}
					throw new FatalListenerStartupException("Illegal Argument on Queue Declaration", e);
				}
			}
			catch (IOException e) {
				if (logger.isWarnEnabled()) {
					logger.warn("Failed to declare queue: " + queueName);
				}
				this.missingQueuePublisher.accept(queueName);
				if (!this.channel.isOpen()) {
					throw new AmqpIOException(e);
				}
				if (failures == null) {
					failures = new DeclarationException(e);
				}
				failures.addFailedQueue(queueName);
			}
		}
		if (failures != null) {
			throw failures;
		}
	}

	public synchronized void stop() {
		if (this.abortStarted == 0) { // signal handle delivery to use offer
			this.abortStarted = System.currentTimeMillis();
		}
		if (!this.cancelled()) {
			try {
				RabbitUtils.closeMessageConsumer(this.channel, getConsumerTags(), this.transactional);
			}
			catch (Exception e) {
				if (logger.isDebugEnabled()) {
					logger.debug("Error closing consumer " + this, e);
				}
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Closing Rabbit Channel: " + this.channel);
		}
		forceCloseAndClearQueue();
	}

	public void forceCloseAndClearQueue() {
		if (this.channel != null && this.channel.isOpen()) {
			RabbitUtils.setPhysicalCloseRequired(this.channel, true);
			ConnectionFactoryUtils.releaseResources(this.resourceHolder);
			this.deliveryTags.clear();
			this.consumers.clear();
			this.queue.clear(); // in case we still have a client thread blocked
		}
	}

	/**
	 * Perform a rollback, handling rollback exceptions properly.
	 * @param ex the thrown application exception or error
	 */
	public void rollbackOnExceptionIfNecessary(Throwable ex) {
		rollbackOnExceptionIfNecessary(ex, -1);
	}

	/**
	 * Perform a rollback, handling rollback exceptions properly.
	 * @param ex the thrown application exception or error
	 * @param tag delivery tag; when specified (greater than or equal to 0) only that
	 * message is nacked.
	 * @since 2.2.21.
	 */
	public void rollbackOnExceptionIfNecessary(Throwable ex, long tag) {

		boolean ackRequired = !this.acknowledgeMode.isAutoAck()
				&& (!this.acknowledgeMode.isManual() || ContainerUtils.isRejectManual(ex));
		try {
			if (this.transactional) {
				if (logger.isDebugEnabled()) {
					logger.debug("Initiating transaction rollback on application exception: " + ex);
				}
				RabbitUtils.rollbackIfNecessary(this.channel);
			}
			if (ackRequired) {
				if (tag < 0) {
					OptionalLong deliveryTag = this.deliveryTags.stream().mapToLong(l -> l).max();
					if (deliveryTag.isPresent()) {
						this.channel.basicNack(deliveryTag.getAsLong(), true,
								ContainerUtils.shouldRequeue(this.defaultRequeueRejected, ex, logger));
					}
					if (this.transactional) {
						// Need to commit the reject (=nack)
						RabbitUtils.commitIfNecessary(this.channel);
					}
				}
				else {
					this.channel.basicNack(tag, false,
							ContainerUtils.shouldRequeue(this.defaultRequeueRejected, ex, logger));
				}
			}
		}
		catch (Exception e) {
			logger.error("Application exception overridden by rollback exception", ex);
			throw RabbitExceptionTranslator.convertRabbitAccessException(e); // NOSONAR stack trace loss
		}
		finally {
			if (tag < 0) {
				this.deliveryTags.clear();
			}
			else {
				this.deliveryTags.remove(tag);
			}
		}
	}

	/**
	 * Perform a commit or message acknowledgement, as appropriate.
	 * @param localTx Whether the channel is locally transacted.
	 * @return true if at least one delivery tag exists.
	 * @throws IOException Any IOException.
	 */
	public boolean commitIfNecessary(boolean localTx) throws IOException {
		if (this.deliveryTags.isEmpty()) {
			return false;
		}

		/*
		 * If we have a TX Manager, but no TX, act like we are locally transacted.
		 */
		boolean isLocallyTransacted = localTx
				|| (this.transactional
				&& TransactionSynchronizationManager.getResource(this.connectionFactory) == null);
		try {

			boolean ackRequired = !this.acknowledgeMode.isAutoAck() && !this.acknowledgeMode.isManual();

			if (ackRequired && (!this.transactional || isLocallyTransacted)) {
				long deliveryTag = new ArrayList<Long>(this.deliveryTags).get(this.deliveryTags.size() - 1);
				try {
					this.channel.basicAck(deliveryTag, true);
					notifyMessageAckListener(true, deliveryTag, null);
				}
				catch (Exception e) {
					logger.error("Error acking.", e);
					notifyMessageAckListener(false, deliveryTag, e);
				}
			}

			if (isLocallyTransacted) {
				// For manual acks we still need to commit
				RabbitUtils.commitIfNecessary(this.channel);
			}

		}
		finally {
			this.deliveryTags.clear();
		}

		return true;

	}

	/**
	 * Notify MessageAckListener set on message listener.
	 * @param success Whether ack succeeded.
	 * @param deliveryTag The deliveryTag of ack.
	 * @param cause If an exception occurs.
	 * @since 2.4.6
	 */
	private void notifyMessageAckListener(boolean success, long deliveryTag, @Nullable Throwable cause) {
		try {
			this.messageAckListener.onComplete(success, deliveryTag, cause);
		}
		catch (Exception e) {
			logger.error("An exception occured in MessageAckListener.", e);
		}
	}

	@Override
	public String toString() {
		return "Consumer@" + ObjectUtils.getIdentityHexString(this) + ": "
				+ "tags=[" + getConsumerTags()
				+ "], channel=" + this.channel
				+ ", acknowledgeMode=" + this.acknowledgeMode + " local queue size=" + this.queue.size();
	}

	private final class InternalConsumer extends DefaultConsumer {

		private final String queueName;

		private boolean canceled;

		InternalConsumer(Channel channel, String queue) {
			super(channel);
			this.queueName = queue;
		}

		@Override
		public void handleConsumeOk(String consumerTag) {
			super.handleConsumeOk(consumerTag);
			if (logger.isDebugEnabled()) {
				logger.debug("ConsumeOK: " + BlockingQueueConsumer.this);
			}
			if (BlockingQueueConsumer.this.applicationEventPublisher != null) {
				BlockingQueueConsumer.this.applicationEventPublisher
						.publishEvent(new ConsumeOkEvent(this, this.queueName, consumerTag));
			}
		}

		@Override
		public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
			if (logger.isDebugEnabled()) {
				if (RabbitUtils.isNormalShutdown(sig)) {
					logger.debug("Received shutdown signal for consumer tag=" + consumerTag + ": " + sig.getMessage());
				}
				else {
					logger.debug("Received shutdown signal for consumer tag=" + consumerTag, sig);
				}
			}
			BlockingQueueConsumer.this.shutdown = sig;
			// The delivery tags will be invalid if the channel shuts down
			BlockingQueueConsumer.this.deliveryTags.clear();
			BlockingQueueConsumer.this.activeObjectCounter.release(BlockingQueueConsumer.this);
		}

		@Override
		public void handleCancel(String consumerTag) {
			if (logger.isWarnEnabled()) {
				logger.warn("Cancel received for " + consumerTag + " ("
						+ this.queueName
						+ "); " + BlockingQueueConsumer.this);
			}
			BlockingQueueConsumer.this.consumers.remove(this.queueName);
			if (!BlockingQueueConsumer.this.consumers.isEmpty()) {
				basicCancel(false);
			}
			else {
				BlockingQueueConsumer.this.cancelled.set(true);
			}
		}

		@Override
		public void handleCancelOk(String consumerTag) {
			if (logger.isDebugEnabled()) {
				logger.debug("Received cancelOk for tag " + consumerTag + " ("
						+ this.queueName
						+ "); " + BlockingQueueConsumer.this);
			}
			this.canceled = true;
		}

		@Override
		public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
				byte[] body) {
			if (logger.isDebugEnabled()) {
				logger.debug("Storing delivery for consumerTag: '"
						+ consumerTag + "' with deliveryTag: '" + envelope.getDeliveryTag() + "' in "
						+ BlockingQueueConsumer.this);
			}
			try {
				if (BlockingQueueConsumer.this.abortStarted > 0) {
					if (!BlockingQueueConsumer.this.queue.offer(
							new Delivery(consumerTag, envelope, properties, body, this.queueName),
							BlockingQueueConsumer.this.shutdownTimeout, TimeUnit.MILLISECONDS)) {

						Channel channelToClose = super.getChannel();
						RabbitUtils.setPhysicalCloseRequired(channelToClose, true);
						// Defensive - should never happen
						BlockingQueueConsumer.this.queue.clear();
						if (!this.canceled) {
							RabbitUtils.cancel(channelToClose, consumerTag);
						}
						try {
							channelToClose.close();
						}
						catch (@SuppressWarnings("unused") TimeoutException e) {
							// no-op
						}
					}
				}
				else {
					BlockingQueueConsumer.this.queue
							.put(new Delivery(consumerTag, envelope, properties, body, this.queueName));
				}
			}
			catch (@SuppressWarnings("unused") InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			catch (Exception e) {
				BlockingQueueConsumer.logger.warn("Unexpected exception during delivery", e);
			}
		}

		@Override
		public String toString() {
			return "InternalConsumer{" + "queue='" + this.queueName + '\'' +
					", consumerTag='" + getConsumerTag() + '\'' +
					'}';
		}

	}

	@SuppressWarnings("serial")
	private static final class DeclarationException extends AmqpException {

		DeclarationException() {
			super("Failed to declare queue(s):");
		}

		private DeclarationException(Throwable t) {
			super("Failed to declare queue(s):", t);
		}

		private final List<String> failedQueues = new ArrayList<String>();

		private void addFailedQueue(String queue) {
			this.failedQueues.add(queue);
		}

		private List<String> getFailedQueues() {
			return this.failedQueues;
		}

		@Override
		public String getMessage() {
			return super.getMessage() + this.failedQueues.toString();
		}

	}

}
