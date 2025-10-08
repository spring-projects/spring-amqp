/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.amqp.rabbitmq.client.listener;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Resource;
import org.aopalliance.aop.Advice;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AmqpAcknowledgment;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.support.ContainerUtils;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpUtils;
import org.springframework.amqp.support.postprocessor.MessagePostProcessorUtils;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.log.LogAccessor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.ObjectUtils;

/**
 * A listener container for RabbitMQ AMQP 1.0 Consumer.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 *
 */
public class RabbitAmqpListenerContainer implements MessageListenerContainer, BeanNameAware, DisposableBean {

	private static final LogAccessor LOG = new LogAccessor(LogFactory.getLog(RabbitAmqpListenerContainer.class));

	private final Lock lock = new ReentrantLock();

	private final AmqpConnectionFactory connectionFactory;

	private final MultiValueMap<String, Consumer> queueToConsumers = new LinkedMultiValueMap<>();

	private String @Nullable [] queues;

	private Advice @Nullable [] adviceChain;

	private int initialCredits = 100;

	private int priority;

	private Resource.StateListener @Nullable [] stateListeners;

	private boolean autoSettle = true;

	private boolean defaultRequeue = true;

	private int consumersPerQueue = 1;

	private @Nullable MessageListener messageListener;

	private @Nullable MessageListener proxy;

	private boolean asyncReplies;

	private ErrorHandler errorHandler = new ConditionalRejectingErrorHandler();

	private @Nullable Collection<MessagePostProcessor> afterReceivePostProcessors;

	private boolean autoStartup = true;

	private String beanName = "not.a.Spring.bean";

	private @Nullable String listenerId;

	private Duration gracefulShutdownPeriod = Duration.ofSeconds(30);

	private int batchSize;

	private Duration batchReceiveDuration = Duration.ofSeconds(30);

	private @Nullable TaskScheduler taskScheduler;

	private boolean internalTaskScheduler = true;

	/**
	 * Construct an instance based on the provided {@link AmqpConnectionFactory}.
	 * @param connectionFactory to use.
	 */
	public RabbitAmqpListenerContainer(AmqpConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	@Override
	public void setQueueNames(String... queueNames) {
		this.queues = Arrays.copyOf(queueNames, queueNames.length);
	}

	/**
	 * The initial number credits to grant to the AMQP receiver.
	 * The default is {@code 100}.
	 * @param initialCredits number of initial credits
	 * @see com.rabbitmq.client.amqp.ConsumerBuilder#initialCredits(int)
	 */
	public void setInitialCredits(int initialCredits) {
		this.initialCredits = initialCredits;
	}

	/**
	 * The consumer priority.
	 * @param priority consumer priority
	 * @see com.rabbitmq.client.amqp.ConsumerBuilder#priority(int)
	 */
	public void setPriority(int priority) {
		this.priority = priority;
	}

	/**
	 * Add {@link Resource.StateListener} instances to the consumer.
	 * @param stateListeners listeners to add
	 * @see com.rabbitmq.client.amqp.ConsumerBuilder#listeners(Resource.StateListener...)
	 */
	public void setStateListeners(Resource.StateListener... stateListeners) {
		this.stateListeners = Arrays.copyOf(stateListeners, stateListeners.length);
	}

	/**
	 * Set {@link MessagePostProcessor}s that will be applied after message reception, before
	 * invoking the {@link MessageListener}. Often used to decompress data.  Processors are invoked in order,
	 * depending on {@code PriorityOrder}, {@code Order} and finally unordered.
	 * @param afterReceivePostProcessors the post processor.
	 */
	public void setAfterReceivePostProcessors(MessagePostProcessor... afterReceivePostProcessors) {
		this.afterReceivePostProcessors = MessagePostProcessorUtils.sort(Arrays.asList(afterReceivePostProcessors));
	}

	/**
	 * Set a number of AMQP messages to gather before producing as a single message downstream.
	 * Default 1 - no batching.
	 * @param batchSize the batch size to use.
	 * @see #setBatchReceiveTimeout(long)
	 */
	public void setBatchSize(int batchSize) {
		Assert.isTrue(batchSize > 1, "'batchSize' must be greater than 1");
		this.batchSize = batchSize;
	}

	/**
	 * Set a timeout in milliseconds for how long a batch gathering process should go.
	 * Therefore, the batch is released as a single message whatever first happens:
	 * this timeout or {@link #setBatchSize(int)}.
	 * Default 30 seconds.
	 * @param batchReceiveTimeout the timeout for gathering a batch.
	 */
	public void setBatchReceiveTimeout(long batchReceiveTimeout) {
		this.batchReceiveDuration = Duration.ofMillis(batchReceiveTimeout);
	}

	/**
	 * Set a {@link TaskScheduler} for monitoring batch releases.
	 * @param taskScheduler the {@link TaskScheduler} to use.
	 */
	public void setTaskScheduler(TaskScheduler taskScheduler) {
		this.taskScheduler = taskScheduler;
		this.internalTaskScheduler = false;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	@Override
	public void setAutoStartup(boolean autoStart) {
		this.autoStartup = autoStart;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	/**
	 * Set an advice chain to apply to the listener.
	 * @param advices the advice chain.
	 */
	public void setAdviceChain(Advice... advices) {
		Assert.notNull(advices, "'advices' cannot be null");
		Assert.noNullElements(advices, "'advices' cannot have null elements");
		this.adviceChain = Arrays.copyOf(advices, advices.length);
	}

	/**
	 * Set to {@code false} to propagate a
	 * {@link org.springframework.amqp.core.MessageProperties#setAmqpAcknowledgment(AmqpAcknowledgment)}
	 * for target {@link MessageListener} manual settlement.
	 * In case of {@link RabbitAmqpMessageListener}, the native {@link Consumer.Context}
	 * should be used for manual settlement.
	 * @param autoSettle to call {@link Consumer.Context#accept()} automatically.
	 */
	public void setAutoSettle(boolean autoSettle) {
		this.autoSettle = autoSettle;
	}

	/**
	 * Set the default behavior when a message processing has failed.
	 * When true, messages will be requeued, when false, they will be discarded.
	 * This option can be overruled by throwing
	 * {@link org.springframework.amqp.AmqpRejectAndDontRequeueException} or
	 * {@link org.springframework.amqp.ImmediateRequeueAmqpException} from the message listener.
	 * Default true.
	 * @param defaultRequeue true to requeue by default.
	 */
	public void setDefaultRequeue(boolean defaultRequeue) {
		this.defaultRequeue = defaultRequeue;
	}

	/**
	 * Set a duration for how long to wait for all the consumers to shut down successfully on listener container stop.
	 * Default 30 seconds.
	 * @param gracefulShutdownPeriod the timeout to wait on stop.
	 */
	public void setGracefulShutdownPeriod(Duration gracefulShutdownPeriod) {
		this.gracefulShutdownPeriod = gracefulShutdownPeriod;
	}

	/**
	 * Each queue runs in its own consumer; set this property to create multiple
	 * consumers for each queue.
	 * Can be treated as {@code concurrency}, but per queue.
	 * @param consumersPerQueue the consumers per queue.
	 */
	public void setConsumersPerQueue(int consumersPerQueue) {
		this.consumersPerQueue = consumersPerQueue;
	}

	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	@Override
	public void setListenerId(String id) {
		this.listenerId = id;
	}

	/**
	 * The 'id' attribute of the listener.
	 * @return the id (or the container bean name if no id set).
	 */
	public String getListenerId() {
		return this.listenerId != null ? this.listenerId : this.beanName;
	}

	@Override
	public void setupMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
		this.asyncReplies = messageListener.isAsyncReplies();
		if (this.messageListener instanceof RabbitAmqpMessageListenerAdapter rabbitAmqpMessageListenerAdapter) {
			rabbitAmqpMessageListenerAdapter.setConnectionFactory(this.connectionFactory);
		}
		this.proxy = this.messageListener;
		if (!ObjectUtils.isEmpty(this.adviceChain)) {
			ProxyFactory factory = new ProxyFactory(messageListener);
			for (Advice advice : this.adviceChain) {
				factory.addAdvisor(new DefaultPointcutAdvisor(advice));
			}
			factory.setInterfaces(messageListener.getClass().getInterfaces());
			this.proxy = (MessageListener) factory.getProxy(getClass().getClassLoader());
		}
	}

	@Override
	public @Nullable Object getMessageListener() {
		return this.proxy;
	}

	@Override
	public void afterPropertiesSet() {
		Assert.state(this.queues != null, "At least one queue has to be provided for consuming.");
		Assert.state(this.messageListener != null, "The 'messageListener' must be provided.");

		if (this.asyncReplies && this.autoSettle) {
			LOG.info("Enforce MANUAL settlement for async replies.");
			this.autoSettle = false;
		}

		this.messageListener.containerAckMode(this.autoSettle ? AcknowledgeMode.AUTO : AcknowledgeMode.MANUAL);
		if (this.messageListener instanceof RabbitAmqpMessageListenerAdapter adapter
				&& this.afterReceivePostProcessors != null) {

			adapter.setAfterReceivePostProcessors(this.afterReceivePostProcessors);
		}

		if (this.batchSize > 1 && this.internalTaskScheduler) {
			ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
			threadPoolTaskScheduler.setThreadNamePrefix(getListenerId() + "-consumerMonitor-");
			threadPoolTaskScheduler.afterPropertiesSet();
			this.taskScheduler = threadPoolTaskScheduler;
		}
	}

	@Override
	public boolean isRunning() {
		this.lock.lock();
		try {
			return !this.queueToConsumers.isEmpty();
		}
		finally {
			this.lock.unlock();
		}
	}

	@Override
	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	public void start() {
		this.lock.lock();
		try {
			if (this.queueToConsumers.isEmpty()) {
				Connection connection = this.connectionFactory.getConnection();
				for (String queue : this.queues) {
					for (int i = 0; i < this.consumersPerQueue; i++) {
						Consumer consumer =
								connection.consumerBuilder()
										.queue(queue)
										.priority(this.priority)
										.initialCredits(this.initialCredits)
										.listeners(this.stateListeners)
										.messageHandler(new ConsumerMessageHandler())
										.build();
						this.queueToConsumers.add(queue, consumer);
					}
				}
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	private void invokeListener(Consumer.Context context, com.rabbitmq.client.amqp.Message amqpMessage) {
		try {
			doInvokeListener(context, amqpMessage);
			if (this.autoSettle) {
				context.accept();
			}
		}
		catch (Exception ex) {
			handleListenerError(ex, context, amqpMessage);
		}
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	private void doInvokeListener(Consumer.Context context, com.rabbitmq.client.amqp.Message amqpMessage) {
		Consumer.@Nullable Context contextToUse = this.autoSettle ? null : context;
		if (this.proxy instanceof RabbitAmqpMessageListener amqpMessageListener) {
			amqpMessageListener.onAmqpMessage(amqpMessage, contextToUse);
		}
		else {
			Message message = RabbitAmqpUtils.fromAmqpMessage(amqpMessage, contextToUse);
			this.proxy.onMessage(message);
		}
	}

	private void invokeBatchListener(Consumer.Context context, List<com.rabbitmq.client.amqp.Message> batch) {
		Consumer.@Nullable Context contextToUse = this.autoSettle ? null : context;
		List<Message> messages =
				batch.stream()
						.map((amqpMessage) -> RabbitAmqpUtils.fromAmqpMessage(amqpMessage, contextToUse))
						.toList();
		try {
			doInvokeBatchListener(messages);
			if (this.autoSettle) {
				context.accept();
			}
		}
		catch (Exception ex) {
			handleListenerError(ex, context, batch);
		}
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	private void doInvokeBatchListener(List<Message> messages) {
		this.proxy.onMessageBatch(messages);
	}

	private void handleListenerError(Exception ex, Consumer.Context context, Object messageOrBatch) {
		try {
			this.errorHandler.handleError(ex);
			// If error handler does not re-throw an exception, re-check original error.
			// If it is not special, treat the error handler outcome as a successful processing result.
			if (!handleSpecialErrors(ex, context)) {
				context.accept();
			}
		}
		catch (Exception rethrow) {
			if (!handleSpecialErrors(rethrow, context)) {
				if (this.defaultRequeue) {
					context.requeue();
				}
				else {
					context.discard();
				}
				LOG.error(rethrow, () ->
						"The 'errorHandler' has thrown an exception. The '" + messageOrBatch + "' is "
								+ (this.defaultRequeue ? "re-queued." : "discarded."));
			}
		}
	}

	private boolean handleSpecialErrors(Exception ex, Consumer.Context context) {
		if (ContainerUtils.shouldRequeue(this.defaultRequeue, ex, LOG.getLog())) {
			context.requeue();
			return true;
		}
		if (ContainerUtils.isAmqpReject(ex)) {
			context.discard();
			return true;
		}
		if (ContainerUtils.isImmediateAcknowledge(ex)) {
			context.accept();
			return true;
		}
		return false;
	}

	@Override
	public void stop() {
		stop(() -> {
		});
	}

	@Override
	@SuppressWarnings("unchecked")
	public void stop(Runnable callback) {
		this.lock.lock();
		try {
			CompletableFuture<Void>[] completableFutures =
					this.queueToConsumers.values().stream()
							.flatMap(List::stream)
							.map((consumer) ->
									CompletableFuture.supplyAsync(() -> {
										consumer.pause();
										try (consumer) {
											while (consumer.unsettledMessageCount() > 0) {
												Thread.sleep(100);
											}
										}
										catch (InterruptedException ex) {
											Thread.currentThread().interrupt();
											throw new RuntimeException(ex);
										}
										return null;
									}))
							.toArray(CompletableFuture[]::new);

			CompletableFuture.allOf(completableFutures)
					.orTimeout(this.gracefulShutdownPeriod.toMillis(), TimeUnit.MILLISECONDS)
					.whenComplete((unused, throwable) -> {
						this.queueToConsumers.clear();
						callback.run();
					});
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Pause all the consumer for all queues.
	 */
	public void pause() {
		this.queueToConsumers.values()
				.stream()
				.flatMap(List::stream)
				.forEach(Consumer::pause);
	}

	/**
	 * Resume all the consumer for all queues.
	 */
	public void resume() {
		this.queueToConsumers.values()
				.stream()
				.flatMap(List::stream)
				.forEach(Consumer::unpause);
	}

	/**
	 * Pause all the consumer for specific queue.
	 */
	public void pause(String queueName) {
		List<Consumer> consumers = this.queueToConsumers.get(queueName);
		if (consumers != null) {
			consumers.forEach(Consumer::pause);
		}
	}

	/**
	 * Resume all the consumer for specific queue.
	 */
	public void resume(String queueName) {
		List<Consumer> consumers = this.queueToConsumers.get(queueName);
		if (consumers != null) {
			consumers.forEach(Consumer::unpause);
		}
	}

	@Override
	public void destroy() {
		if (this.internalTaskScheduler && this.taskScheduler != null) {
			((ThreadPoolTaskScheduler) this.taskScheduler).shutdown();
		}
	}

	private class ConsumerMessageHandler implements Consumer.MessageHandler {

		private volatile @Nullable ConsumerBatch consumerBatch;

		ConsumerMessageHandler() {
		}

		@Override
		public void handle(Consumer.Context context, com.rabbitmq.client.amqp.Message message) {
			if (RabbitAmqpListenerContainer.this.batchSize > 1) {
				ConsumerBatch currentBatch = this.consumerBatch;
				if (currentBatch == null || currentBatch.batchReleaseFuture == null) {
					currentBatch = new ConsumerBatch(context.batch(RabbitAmqpListenerContainer.this.batchSize));
					this.consumerBatch = currentBatch;
				}
				currentBatch.add(context, message);
				if (currentBatch.batchContext.size() == RabbitAmqpListenerContainer.this.batchSize) {
					currentBatch.release();
					this.consumerBatch = null;
				}
			}
			else {
				invokeListener(context, message);
			}
		}

		private class ConsumerBatch {

			private final List<com.rabbitmq.client.amqp.Message> batch = new ArrayList<>();

			private final Consumer.BatchContext batchContext;

			private volatile @Nullable ScheduledFuture<?> batchReleaseFuture;

			ConsumerBatch(Consumer.BatchContext batchContext) {
				this.batchContext = batchContext;
			}

			void add(Consumer.Context context, com.rabbitmq.client.amqp.Message message) {
				this.batchContext.add(context);
				this.batch.add(message);
				if (this.batchReleaseFuture == null) {
					this.batchReleaseFuture =
							Objects.requireNonNull(RabbitAmqpListenerContainer.this.taskScheduler)
									.schedule(this::releaseInternal,
											Instant.now().plus(RabbitAmqpListenerContainer.this.batchReceiveDuration));
				}
			}

			void release() {
				ScheduledFuture<?> currentBatchReleaseFuture = this.batchReleaseFuture;
				if (currentBatchReleaseFuture != null) {
					currentBatchReleaseFuture.cancel(true);
					releaseInternal();
				}
			}

			private void releaseInternal() {
				if (this.batchReleaseFuture != null) {
					this.batchReleaseFuture = null;
					invokeBatchListener(this.batchContext, this.batch);
				}
			}

		}

	}

}
