/*
 * Copyright 2025 the original author or authors.
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Resource;
import org.aopalliance.aop.Advice;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AmqpAcknowledgment;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.support.ContainerUtils;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpUtils;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.core.log.LogAccessor;
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
public class RabbitAmqpListenerContainer implements MessageListenerContainer {

	private static final LogAccessor LOG = new LogAccessor(LogFactory.getLog(RabbitAmqpListenerContainer.class));

	private final Lock lock = new ReentrantLock();

	private final Connection connection;

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

	private ErrorHandler errorHandler = new ConditionalRejectingErrorHandler();

	private boolean autoStartup = true;

	private @Nullable String listenerId;

	private Duration gracefulShutdownPeriod = Duration.ofSeconds(30);

	/**
	 * Construct an instance using the provided connection.
	 * @param connection to use.
	 */
	public RabbitAmqpListenerContainer(Connection connection) {
		this.connection = connection;
	}

	@Override
	public void setQueueNames(String... queueNames) {
		this.queues = Arrays.copyOf(queueNames, queueNames.length);
	}

	public void setInitialCredits(int initialCredits) {
		this.initialCredits = initialCredits;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public void setStateListeners(Resource.StateListener... stateListeners) {
		this.stateListeners = Arrays.copyOf(stateListeners, stateListeners.length);
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
	 * @since 2.4.5
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
	 * When true, the default can be overridden by the listener throwing an
	 * {@link AmqpRejectAndDontRequeueException}. Default true.
	 * @param defaultRequeue true to requeue by default.
	 */
	public void setDefaultRequeue(boolean defaultRequeue) {
		this.defaultRequeue = defaultRequeue;
	}

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

	@Override
	public void setupMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
		if (!ObjectUtils.isEmpty(this.adviceChain)) {
			ProxyFactory factory = new ProxyFactory(messageListener);
			for (Advice advice : this.adviceChain) {
				factory.addAdvisor(new DefaultPointcutAdvisor(advice));
			}
			factory.setInterfaces(messageListener.getClass().getInterfaces());
			this.messageListener = (MessageListener) factory.getProxy(getClass().getClassLoader());
		}
	}

	@Override
	public @Nullable Object getMessageListener() {
		return this.messageListener;
	}

	@Override
	public void afterPropertiesSet() {
		Assert.state(this.queues != null, "At least one queue has to be provided for consuming.");
		Assert.state(this.messageListener != null, "The 'messageListener' must be provided.");

		this.messageListener.containerAckMode(this.autoSettle ? AcknowledgeMode.AUTO : AcknowledgeMode.MANUAL);
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
				for (String queue : this.queues) {
					for (int i = 0; i < this.consumersPerQueue; i++) {
						Consumer consumer =
								this.connection.consumerBuilder()
										.queue(queue)
										.priority(this.priority)
										.initialCredits(this.initialCredits)
										.listeners(this.stateListeners)
										.messageHandler(this::invokeListener)
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
							"The 'errorHandler' has thrown an exception. The '" + amqpMessage + "' is "
									+ (this.defaultRequeue ? "re-queued." : "discarded."));
				}
			}
		}
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	private void doInvokeListener(Consumer.Context context, com.rabbitmq.client.amqp.Message amqpMessage) {
		Consumer.@Nullable Context contextToUse = this.autoSettle ? null : context;
		if (this.messageListener instanceof RabbitAmqpMessageListener amqpMessageListener) {
			amqpMessageListener.onAmqpMessage(amqpMessage, contextToUse);
		}
		else {
			Message message = RabbitAmqpUtils.fromAmqpMessage(amqpMessage, contextToUse);
			this.messageListener.onMessage(message);
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

}
