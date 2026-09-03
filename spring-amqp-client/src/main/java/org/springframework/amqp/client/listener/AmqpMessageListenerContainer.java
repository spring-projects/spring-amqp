/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client.listener;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.aopalliance.aop.Advice;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientReceiver;
import org.apache.qpid.protonj2.engine.Scheduler;
import org.apache.qpid.protonj2.engine.impl.ProtonLinkCreditState;
import org.apache.qpid.protonj2.engine.impl.ProtonReceiver;
import org.apache.qpid.protonj2.engine.impl.ProtonSessionIncomingWindow;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.client.AmqpConnectionFactory;
import org.springframework.amqp.client.ProtonUtils;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AmqpAcknowledgment;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageListenerContainer;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

/**
 * The {@link MessageListenerContainer} implementation for AMQP 1.0 protocol.
 * <p>
 * When {@link #autoAccept} is {@code false},
 * an {@link org.springframework.amqp.core.MessageProperties#setAmqpAcknowledgment(AmqpAcknowledgment)}
 * is populated to the Spring AMQP message to be handled by the listener.
 * Therefore, the listener must manually acknowledge the message or reject/requeue, according to its logic.
 * <p>
 * If a {@link ProtonDeliveryListener} is provided and {@code autoAccept == false},
 * it is the listener's responsibility to acknowledge the message manually and replenish the link credits.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class AmqpMessageListenerContainer implements MessageListenerContainer, BeanNameAware {

	private static final LogAccessor LOG = new LogAccessor(AmqpMessageListenerContainer.class);

	private static final Duration DEFAULT_RECOVERY_INTERVAL = Duration.ofSeconds(5);

	private static final long RECOVERY_SLEEP_SLICE = 100;

	private static final long PAUSE_TIMEOUT_MILLIS = 10_000;

	private final Lock lock = new ReentrantLock();

	private final AmqpConnectionFactory connectionFactory;

	private final MultiValueMap<String, AmqpConsumer> queueToConsumers = new LinkedMultiValueMap<>();

	private @Nullable MessageListener messageListener;

	@SuppressWarnings("NullAway.Init")
	private MessageListener proxy;

	@SuppressWarnings("NullAway.Init")
	private String[] queues;

	private Advice @Nullable [] adviceChain;

	private int consumersPerQueue = 1;

	private String beanName = "not.a.Spring.bean";

	private @Nullable ErrorHandler errorHandler;

	private boolean autoStartup = true;

	private Duration receiveTimeout = Duration.ofSeconds(1);

	private Duration gracefulShutdownPeriod = Duration.ofSeconds(30);

	private BackOff recoveryBackOff =
			new FixedBackOff(DEFAULT_RECOVERY_INTERVAL.toMillis(), FixedBackOff.UNLIMITED_ATTEMPTS);

	private Executor taskExecutor = new SimpleAsyncTaskExecutor();

	private boolean taskExecutorSet;

	private boolean autoAccept = true;

	private int initialCredits = 100;

	private @Nullable String listenerId;

	private boolean asyncReplies;

	public AmqpMessageListenerContainer(AmqpConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	@Override
	public void setupMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
		this.asyncReplies = messageListener.isAsyncReplies();

		if (!ObjectUtils.isEmpty(this.adviceChain)) {
			ProxyFactory factory = new ProxyFactory(messageListener);
			for (Advice advice : this.adviceChain) {
				factory.addAdvisor(new DefaultPointcutAdvisor(advice));
			}
			factory.setInterfaces(messageListener.getClass().getInterfaces());
			this.proxy = (MessageListener) factory.getProxy(getClass().getClassLoader());
		}
		else {
			this.proxy = messageListener;
		}
	}

	@Override
	public @Nullable Object getMessageListener() {
		return this.proxy;
	}

	@Override
	public void setQueueNames(String... queues) {
		this.queues = Arrays.copyOf(queues, queues.length);
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

	/**
	 * Set to {@code false} to propagate a
	 * {@link org.springframework.amqp.core.MessageProperties#setAmqpAcknowledgment(AmqpAcknowledgment)}
	 * for target {@link MessageListener} manual acknowledgement.
	 * @param autoAccept to call {@link Delivery#accept()} automatically by the ProtonJ Client.
	 */
	public void setAutoAccept(boolean autoAccept) {
		this.autoAccept = autoAccept;
	}

	/**
	 * The initial number of credits to grant to the AMQP receiver.
	 * The default is {@code 100}.
	 * @param initialCredits number of initial credits
	 * @see Receiver#addCredit(int)
	 */
	public void setInitialCredits(int initialCredits) {
		this.initialCredits = initialCredits;
	}

	/**
	 * Set an advice chain to apply to the listener.
	 * @param advices the advice chain.
	 */
	public void setAdviceChain(Advice... advices) {
		this.adviceChain = Arrays.copyOf(advices, advices.length);
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

	/**
	 * Set a task executor to run consumers.
	 * @param taskExecutor the task executor.
	 */
	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
		this.taskExecutorSet = true;
	}

	/**
	 * Set the timeout for deliveries from the broker in the {@link AmqpConsumer}.
	 * Default 1 second.
	 * @param receiveTimeout the timeout waiting for deliveries in the consumer.
	 */
	public void setReceiveTimeout(Duration receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	/**
	 * Set an interval between recovery attempts of a failed consumer.
	 * Default 5 seconds.
	 * @param recoveryInterval the interval between recovery attempts.
	 * @since 4.1.1
	 * @see #setRecoveryBackOff(BackOff)
	 */
	public void setRecoveryInterval(Duration recoveryInterval) {
		setRecoveryBackOff(new FixedBackOff(recoveryInterval.toMillis(), FixedBackOff.UNLIMITED_ATTEMPTS));
	}

	/**
	 * Specify the {@link BackOff} for a failed consumer recovery.
	 * The receiver link is not restored by the ProtonJ client itself, even when the connection
	 * is re-established, so the consumer re-opens its receiver according to this {@link BackOff}.
	 * The consumer is stopped when the {@link BackOff} returns {@link BackOffExecution#STOP},
	 * and then the {@link #isRunning()} returns {@code false}
	 * as soon as no consumers are left in this container.
	 * The default is {@link FixedBackOff} with a 5-second interval and unlimited attempts.
	 * @param recoveryBackOff the {@link BackOff} to use for the consumer recovery.
	 * @since 4.1.1
	 */
	public void setRecoveryBackOff(BackOff recoveryBackOff) {
		this.recoveryBackOff = recoveryBackOff;
	}

	@Override
	public void afterPropertiesSet() {
		Assert.state(this.queues != null, "At least one queue has to be provided for consuming.");
		Assert.state(this.messageListener != null, "The 'messageListener' must be provided.");

		if (this.asyncReplies && this.autoAccept) {
			LOG.info("Enforce MANUAL settlement for async replies.");
			this.autoAccept = false;
		}

		this.messageListener.containerAckMode(this.autoAccept ? AcknowledgeMode.NONE : AcknowledgeMode.MANUAL);

		String listenerIdToUse = getListenerId();
		if (!this.taskExecutorSet && StringUtils.hasText(listenerIdToUse)) {
			this.taskExecutor = new SimpleAsyncTaskExecutor(listenerIdToUse + "-");
		}
	}

	@Override
	public void setAutoStartup(boolean autoStart) {
		this.autoStartup = autoStart;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
	public void start() {
		this.lock.lock();
		try {
			if (this.queueToConsumers.isEmpty()) {
				for (String queue : this.queues) {
					for (int i = 0; i < this.consumersPerQueue; i++) {
						AmqpConsumer consumer = new AmqpConsumer(queue, openReceiver(queue));
						this.queueToConsumers.add(queue, consumer);
						this.taskExecutor.execute(consumer);
					}
				}
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Open a new receiver link for the provided queue with the initial credits granted.
	 * @param queue the queue to consume from.
	 * @return a newly opened receiver.
	 */
	private ClientReceiver openReceiver(String queue) {
		ReceiverOptions receiverOptions =
				new ReceiverOptions()
						// Since 'AmqpConsumer' implements pause/resume logic,
						// the auto-replenishment for the credit window is disabled.
						.creditWindow(0)
						.autoAccept(this.autoAccept);

		Receiver receiver;
		try {
			receiver = this.connectionFactory.getConnection().openReceiver(queue, receiverOptions);
		}
		catch (ClientException ex) {
			throw ProtonUtils.toAmqpException(ex);
		}

		try {
			Future<Receiver> openFuture = receiver.openFuture();
			return (ClientReceiver) ProtonUtils.toSupplier(openFuture, receiverOptions.openTimeout())
					.get()
					.addCredit(this.initialCredits);
		}
		catch (Exception ex) {
			// The link has been created locally but has not reached a usable state:
			// close it, so a failed recovery attempt does not leak a link into the session.
			try {
				receiver.close();
			}
			catch (Exception closeEx) {
				LOG.debug(closeEx, () -> "Failed to close a not opened receiver for: " + queue);
			}
			throw ProtonUtils.toAmqpException(ex);
		}
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
											while (consumer.queuedDeliveries() > 0) {
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

	/**
	 * Pause all the consumers for all queues.
	 */
	public void pause() {
		this.queueToConsumers.values()
				.stream()
				.flatMap(List::stream)
				.forEach(AmqpConsumer::pause);
	}

	/**
	 * Resume all the consumers for all queues.
	 */
	public void resume() {
		this.queueToConsumers.values()
				.stream()
				.flatMap(List::stream)
				.forEach(AmqpConsumer::resume);
	}

	/**
	 * Pause all the consumers for the specific queue.
	 * @param queueName the queue to pause consumers.
	 */
	public void pause(String queueName) {
		List<AmqpConsumer> consumers = this.queueToConsumers.get(queueName);
		if (consumers != null) {
			consumers.forEach(AmqpConsumer::pause);
		}
	}

	/**
	 * Resume all the consumers for the specific queue.
	 * @param queueName the queue to resume consumers.
	 */
	public void resume(String queueName) {
		List<AmqpConsumer> consumers = this.queueToConsumers.get(queueName);
		if (consumers != null) {
			consumers.forEach(AmqpConsumer::resume);
		}
	}

	private void doInvokeListener(Delivery delivery, Runnable replenishCreditOperation) throws Exception {
		AtomicBoolean acknowledged = new AtomicBoolean();
		AmqpAcknowledgment amqpAcknowledgment = null;
		if (!this.autoAccept) {
			amqpAcknowledgment = (status) -> {
				try {
					switch (status) {
						case ACCEPT -> delivery.accept();
						case REJECT -> delivery.reject(null, null);
						case REQUEUE -> delivery.release();
					}
				}
				catch (ClientException ex) {
					throw ProtonUtils.toAmqpException(ex);
				}
				finally {
					acknowledged.set(true);
					replenishCreditOperation.run();
				}
			};
		}

		try {
			if (this.proxy instanceof ProtonDeliveryListener protonDeliveryListener) {
				if (protonDeliveryListener instanceof AcknowledgingProtonDeliveryListener ackProtonDeliveryListener) {
					ackProtonDeliveryListener.onDelivery(delivery, amqpAcknowledgment);
				}
				else {
					protonDeliveryListener.onDelivery(delivery);
				}
			}
			else {
				Message message = ProtonUtils.fromProtonMessage(delivery.message());
				if (amqpAcknowledgment != null) {
					message.getMessageProperties()
							.setAmqpAcknowledgment(amqpAcknowledgment);
				}
				this.proxy.onMessage(message);
			}
		}
		catch (Exception ex) {
			if (this.autoAccept) {
				replenishCreditOperation.run();
			}
			else if (!acknowledged.get()) {
				releaseFailedDelivery(delivery, replenishCreditOperation);
			}
			throw ex;
		}

		if (this.autoAccept) {
			replenishCreditOperation.run();
		}
	}

	/**
	 * Remove a consumer which has stopped without a chance to recover,
	 * so the {@link #isRunning()} does not report this container as running
	 * when all its consumers are gone.
	 * @param queue the queue the consumer was consuming from.
	 * @param consumer the consumer to remove.
	 */
	private void removeConsumer(String queue, AmqpConsumer consumer) {
		this.lock.lock();
		try {
			List<AmqpConsumer> consumers = this.queueToConsumers.get(queue);
			if (consumers != null) {
				consumers.remove(consumer);
				if (consumers.isEmpty()) {
					this.queueToConsumers.remove(queue);
				}
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	private static void releaseFailedDelivery(Delivery delivery, Runnable replenishCreditOperation) {
		try {
			delivery.release();
		}
		catch (ClientException ex) {
			LOG.debug(ex, "Error releasing delivery after listener exception");
		}
		finally {
			replenishCreditOperation.run();
		}
	}

	private class AmqpConsumer implements SchedulingAwareRunnable, AutoCloseable {

		@SuppressWarnings("NullAway")
		private static final Method PROTON_RECEIVER_METHOD =
				ReflectionUtils.findMethod(ClientReceiver.class, "protonLink");

		@SuppressWarnings("NullAway")
		private static final Method CREDIT_STATE_METHOD =
				ReflectionUtils.findMethod(ProtonReceiver.class, "getCreditState");

		@SuppressWarnings("NullAway")
		private static final Field SESSION_WINDOW_FIELD =
				ReflectionUtils.findField(ProtonReceiver.class, "sessionWindow");

		@SuppressWarnings("NullAway")
		private static final Method WRITE_FLOW_METHOD =
				ReflectionUtils.findMethod(ProtonSessionIncomingWindow.class, "writeFlow", ProtonReceiver.class);

		@SuppressWarnings("NullAway")
		private static final Field SCHEDULER_FIELD = ReflectionUtils.findField(ClientReceiver.class, "executor");

		static {
			ReflectionUtils.makeAccessible(PROTON_RECEIVER_METHOD);
			ReflectionUtils.makeAccessible(CREDIT_STATE_METHOD);
			ReflectionUtils.makeAccessible(SESSION_WINDOW_FIELD);
			ReflectionUtils.makeAccessible(WRITE_FLOW_METHOD);
			ReflectionUtils.makeAccessible(SCHEDULER_FIELD);
		}

		private final String queue;

		private final Lock receiverLock = new ReentrantLock();

		@SuppressWarnings("NullAway.Init")
		private volatile ClientReceiver receiver;

		@SuppressWarnings("NullAway.Init")
		private volatile ProtonReceiver protonReceiver;

		@SuppressWarnings("NullAway.Init")
		private volatile ProtonLinkCreditState creditState;

		@SuppressWarnings("NullAway.Init")
		private volatile ProtonSessionIncomingWindow sessionWindow;

		/**
		 * The session serializer this receiver is bound to.
		 * Every mutation of the ProtonJ engine state has to be performed on this thread.
		 */
		@SuppressWarnings("NullAway.Init")
		private volatile Scheduler executor;

		private @Nullable BackOffExecution backOffExecution;

		private volatile boolean paused;

		private volatile boolean running = true;

		AmqpConsumer(String queue, ClientReceiver receiver) {
			this.queue = queue;
			assignReceiver(receiver);
		}

		/**
		 * Adopt the provided receiver together with its internal ProtonJ state,
		 * which is used for the {@code pause()}/{@code resume()} and credit top-up logic.
		 * @param receiverToUse the receiver to consume from.
		 */
		@SuppressWarnings("NullAway")
		private void assignReceiver(ClientReceiver receiverToUse) {
			this.receiver = receiverToUse;
			this.protonReceiver = (ProtonReceiver) ReflectionUtils.invokeMethod(PROTON_RECEIVER_METHOD, receiverToUse);
			this.creditState =
					(ProtonLinkCreditState) ReflectionUtils.invokeMethod(CREDIT_STATE_METHOD, this.protonReceiver);
			this.sessionWindow =
					(ProtonSessionIncomingWindow) ReflectionUtils.getField(SESSION_WINDOW_FIELD, this.protonReceiver);
			this.executor = (Scheduler) ReflectionUtils.getField(SCHEDULER_FIELD, receiverToUse);
		}

		int queuedDeliveries() {
			try {
				return (int) this.receiver.queuedDeliveries();
			}
			catch (Exception ex) {
				// A closed or broken receiver has nothing pending to wait for on stop.
				LOG.debug(ex, () -> "Failed to obtain queued deliveries for: " + this.queue);
				return 0;
			}
		}

		@Override
		public void run() {
			this.receiverLock.lock();
			try {
				while (this.running) {
					Delivery delivery;
					try {
						delivery = this.receiver.receive(AmqpMessageListenerContainer.this.receiveTimeout.toMillis(),
								TimeUnit.MILLISECONDS);
						// A completed receive (even with no delivery) means the link is healthy again.
						this.backOffExecution = null;
					}
					catch (Exception ex) {
						if (!this.running) {
							LOG.debug(ex, "Consumer stopped");
							break;
						}
						AmqpException amqpException = ProtonUtils.toAmqpException(ex);
						if (!handleError(amqpException)) {
							LOG.error(amqpException, () -> "Failed to receive from: " + this.queue);
						}
						// The receiver link is broken and is not restored by the ProtonJ client:
						// re-open it with a back off instead of spinning on the same failure.
						if (!recoverReceiver()) {
							break;
						}
						continue;
					}

					if (delivery != null) {
						try {
							doInvokeListener(delivery, this::replenishCredit);
						}
						catch (Exception ex) {
							if (this.running) {
								AmqpException amqpException = ProtonUtils.toAmqpException(ex);
								if (!handleError(amqpException)) {
									throw amqpException;
								}
							}
							else {
								LOG.debug(ex, "Consumer stopped");
							}
						}
					}
				}
			}
			finally {
				this.receiverLock.unlock();
				if (this.running) {
					// This consumer has given up: deregister it so the container does not
					// report itself as running while nothing is consuming anymore.
					this.running = false;
					closeReceiver();
					removeConsumer(this.queue, this);
				}
			}
		}

		/**
		 * Invoke the container {@link ErrorHandler}, if any.
		 * @param ex the exception to handle.
		 * @return true if the exception has been handled by an {@link ErrorHandler}.
		 */
		private boolean handleError(AmqpException ex) {
			ErrorHandler errorHandlerToUse = AmqpMessageListenerContainer.this.errorHandler;
			if (errorHandlerToUse != null) {
				errorHandlerToUse.handleError(ex);
				return true;
			}
			return false;
		}

		/**
		 * Close the broken receiver and open a new one for the same queue,
		 * according to the {@code recoveryBackOff}.
		 * @return true if a new receiver has been opened; false if this consumer has to stop.
		 */
		private boolean recoverReceiver() {
			closeReceiver();

			BackOffExecution backOffExecutionToUse = this.backOffExecution;
			if (backOffExecutionToUse == null) {
				backOffExecutionToUse = AmqpMessageListenerContainer.this.recoveryBackOff.start();
				this.backOffExecution = backOffExecutionToUse;
			}

			while (this.running) {
				long interval = backOffExecutionToUse.nextBackOff();
				if (interval == BackOffExecution.STOP) {
					LOG.error(() -> "Cannot recover a consumer for: " + this.queue + ". Stopping it.");
					return false;
				}
				if (!sleep(interval)) {
					return false;
				}
				try {
					assignReceiver(openReceiver(this.queue));
					if (this.paused) {
						doPause();
					}
					this.backOffExecution = null;
					LOG.info(() -> "Recovered a consumer for: " + this.queue);
					return true;
				}
				catch (Exception ex) {
					LOG.debug(ex, () -> "Failed to recover a consumer for: " + this.queue);
				}
			}
			return false;
		}

		/**
		 * Sleep for the provided interval in short slices to remain responsive to a container stop.
		 * @param interval how long to sleep, in milliseconds.
		 * @return true if the whole interval has elapsed and this consumer is still running.
		 */
		private boolean sleep(long interval) {
			long deadline = System.currentTimeMillis() + interval;
			try {
				while (this.running) {
					long remaining = deadline - System.currentTimeMillis();
					if (remaining <= 0) {
						return true;
					}
					Thread.sleep(Math.min(remaining, RECOVERY_SLEEP_SLICE));
				}
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			return false;
		}

		private void closeReceiver() {
			try {
				this.receiver.close();
			}
			catch (Exception ex) {
				LOG.debug(ex, () -> "Failed to close a receiver for: " + this.queue);
			}
		}

		/**
		 * Logic copied from {@code ClientReceiver#replenishCreditIfNeeded()}.
		 */
		private void replenishCredit() {
			if (!this.paused && this.running) {
				try {
					int currentCredit = this.protonReceiver.getCredit();
					if (currentCredit <= AmqpMessageListenerContainer.this.initialCredits * 0.5) {
						int potentialPrefetch = currentCredit + queuedDeliveries();

						if (potentialPrefetch <= AmqpMessageListenerContainer.this.initialCredits * 0.7) {
							int additionalCredit = AmqpMessageListenerContainer.this.initialCredits - potentialPrefetch;

							if (!this.paused && this.running) {
								this.receiver.addCredit(additionalCredit);
							}
						}
					}
				}
				catch (ClientException ex) {
					LOG.debug(ex, "Error during credit top-up");
				}
			}
		}

		/**
		 * There is no native 'pause' implementation in the ProtonJ,
		 * so rely on the reflection to imitate behavior with resetting credits to zero.
		 */
		void pause() {
			if (this.running && !this.paused) {
				this.paused = true;
				doPause();
			}
		}

		/**
		 * Zero out the link credit and push the resulting {@code flow} frame to the broker.
		 * <p>
		 * This has to be performed on the session serializer thread: the {@code ProtonSession}
		 * writes every {@code flow} through a single mutable {@code cachedFlow} instance which it
		 * resets, populates and only then hands over to the engine.
		 * Performing that from any other thread lets a concurrent reset land between the field
		 * population and the write, putting an empty {@code flow} performative on the wire.
		 * The mandatory fields of such a frame are all null, so the broker rejects it with an
		 * internal error and closes the whole session, taking down every other link on it.
		 * <p>
		 * The caller is blocked until the credit is really withdrawn, since the {@code pause()}
		 * contract is that no more messages are delivered as soon as it returns.
		 */
		private void doPause() {
			CountDownLatch pauseLatch = new CountDownLatch(1);
			try {
				this.executor.execute(() -> {
					try {
						this.creditState.updateCredit(0);
						ReflectionUtils.invokeMethod(WRITE_FLOW_METHOD, this.sessionWindow, this.protonReceiver);
					}
					finally {
						pauseLatch.countDown();
					}
				});
			}
			catch (RuntimeException ex) {
				// The session serializer is gone (the connection is closing): nothing left to pause.
				LOG.debug(ex, () -> "Failed to pause a consumer for: " + this.queue);
				return;
			}

			try {
				if (!pauseLatch.await(PAUSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
					LOG.debug(() -> "Timed out waiting to pause a consumer for: " + this.queue);
				}
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}

		void resume() {
			if (this.running && this.paused) {
				this.paused = false;
				try {
					this.receiver.addCredit(AmqpMessageListenerContainer.this.initialCredits);
				}
				catch (ClientException ex) {
					throw ProtonUtils.toAmqpException(ex);
				}
			}
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void close() {
			this.running = false;
			this.receiverLock.lock();
			try {
				closeReceiver();
			}
			finally {
				this.receiverLock.unlock();
			}
		}

	}

}
