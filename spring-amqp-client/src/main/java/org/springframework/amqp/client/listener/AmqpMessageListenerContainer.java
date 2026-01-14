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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.LogFactory;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientReceiver;
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

/**
 * The {@link MessageListenerContainer} implementation for AMQP 1.0 protocol.
 * <p>
 * When {@link #autoAccept} is {@code false},
 * an {@link org.springframework.amqp.core.MessageProperties#setAmqpAcknowledgment(AmqpAcknowledgment)}
 * is populated to the Spring AMQP message to be handled by the listener.
 * Therefore, the listener must manually acknowledge the message or reject/requeue, according to its logic.
 * <p>
 * If a {@link ProtonDeliveryListener} is provided and {@code autoAccept == false},
 * it is this listener's responsibility to acknowledge the message manually and replenish the link credits.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class AmqpMessageListenerContainer implements MessageListenerContainer, BeanNameAware {

	private static final LogAccessor LOG = new LogAccessor(LogFactory.getLog(AmqpMessageListenerContainer.class));

	private final Lock lock = new ReentrantLock();

	private final AmqpConnectionFactory connectionFactory;

	private final MultiValueMap<String, AmqpConsumer> queueToConsumers = new LinkedMultiValueMap<>();

	@SuppressWarnings("NullAway.Init")
	private String[] queues;

	private Advice @Nullable [] adviceChain;

	private int consumersPerQueue = 1;

	private @Nullable MessageListener messageListener;

	@SuppressWarnings("NullAway.Init")
	private MessageListener proxy;

	private boolean asyncReplies;

	private @Nullable ErrorHandler errorHandler;

	private boolean autoStartup = true;

	private String beanName = "not.a.Spring.bean";

	private @Nullable String listenerId;

	private Duration receiveTimeout = Duration.ofSeconds(1);

	private Duration gracefulShutdownPeriod = Duration.ofSeconds(30);

	private Executor taskExecutor = new SimpleAsyncTaskExecutor();

	private boolean taskExecutorSet;

	private boolean autoAccept = true;

	private int initialCredits = 100;

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

	public void setErrorHandler(@Nullable ErrorHandler errorHandler) {
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
				Connection connection = this.connectionFactory.getConnection();
				ReceiverOptions receiverOptions =
						new ReceiverOptions()
								// Since 'AmqpConsumer' implements pause/resume logic,
								// the auto-replenishment for the credit window is disabled.
								.creditWindow(0)
								.autoAccept(this.autoAccept);

				for (String queue : this.queues) {
					for (int i = 0; i < this.consumersPerQueue; i++) {
						try {
							ClientReceiver receiver =
									(ClientReceiver) connection.openReceiver(queue, receiverOptions)
											.addCredit(this.initialCredits);
							AmqpConsumer consumer = new AmqpConsumer(receiver);
							this.queueToConsumers.add(queue, consumer);
							this.taskExecutor.execute(consumer);
						}
						catch (ClientException ex) {
							throw ProtonUtils.toAmqpException(ex);
						}
					}
				}
			}
		}
		finally {
			this.lock.unlock();
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

	private void doInvokeListener(Delivery delivery, Runnable replenishCreditOperation)
			throws ClientException {

		if (this.proxy instanceof ProtonDeliveryListener protonDeliveryListener) {
			protonDeliveryListener.onDelivery(delivery);
		}
		else {
			Message message = ProtonUtils.fromProtonMessage(delivery.message());
			if (!this.autoAccept) {
				message.getMessageProperties()
						.setAmqpAcknowledgment((status) -> {
							try {
								switch (status) {
									case ACCEPT -> delivery.accept();
									case REJECT -> delivery.reject(null, null);
									case REQUEUE -> delivery.release();
								}
								replenishCreditOperation.run();
							}
							catch (ClientException ex) {
								throw ProtonUtils.toAmqpException(ex);
							}
						});
			}
			this.proxy.onMessage(message);
		}

		if (this.autoAccept) {
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

		static {
			ReflectionUtils.makeAccessible(PROTON_RECEIVER_METHOD);
			ReflectionUtils.makeAccessible(CREDIT_STATE_METHOD);
			ReflectionUtils.makeAccessible(SESSION_WINDOW_FIELD);
			ReflectionUtils.makeAccessible(WRITE_FLOW_METHOD);
		}

		private final ClientReceiver receiver;

		private final ProtonReceiver protonReceiver;

		private final ProtonLinkCreditState creditState;

		private final ProtonSessionIncomingWindow sessionWindow;

		private volatile boolean paused;

		private volatile boolean running = true;

		@SuppressWarnings("NullAway")
		AmqpConsumer(ClientReceiver receiver) {
			this.receiver = receiver;
			this.protonReceiver = (ProtonReceiver) ReflectionUtils.invokeMethod(PROTON_RECEIVER_METHOD, receiver);
			this.creditState =
					(ProtonLinkCreditState) ReflectionUtils.invokeMethod(CREDIT_STATE_METHOD, this.protonReceiver);
			this.sessionWindow =
					(ProtonSessionIncomingWindow) ReflectionUtils.getField(SESSION_WINDOW_FIELD, this.protonReceiver);
		}

		int queuedDeliveries() {
			return (int) this.receiver.queuedDeliveries();
		}

		@Override
		public void run() {
			while (this.running) {
				try {
					Delivery delivery =
							this.receiver.receive(AmqpMessageListenerContainer.this.receiveTimeout.toMillis(),
									TimeUnit.MILLISECONDS);
					if (delivery != null) {
						doInvokeListener(delivery, this::replenishCredit);
					}
				}
				catch (Exception ex) {
					if (this.running) {
						AmqpException amqpException = ProtonUtils.toAmqpException(ex);
						ErrorHandler errorHandlerToUse = AmqpMessageListenerContainer.this.errorHandler;
						if (errorHandlerToUse != null) {
							errorHandlerToUse.handleError(amqpException);
						}
						else {
							throw amqpException;
						}
					}
					else {
						LOG.debug(ex, "Consumer stopped");
					}
				}
			}
		}

		/**
		 * Logic copied from {@code ClientReceiver#replenishCreditIfNeeded()}.
		 */
		private void replenishCredit() {
			if (!this.paused) {
				try {
					int currentCredit = this.protonReceiver.getCredit();
					if (currentCredit <= AmqpMessageListenerContainer.this.initialCredits * 0.5) {
						int potentialPrefetch = currentCredit + queuedDeliveries();

						if (potentialPrefetch <= AmqpMessageListenerContainer.this.initialCredits * 0.7) {
							int additionalCredit = AmqpMessageListenerContainer.this.initialCredits - potentialPrefetch;

							this.receiver.addCredit(additionalCredit);
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
			this.paused = true;
			this.creditState.updateCredit(0);
			ReflectionUtils.invokeMethod(WRITE_FLOW_METHOD, this.sessionWindow, this.protonReceiver);
		}

		void resume() {
			this.paused = false;
			try {
				this.receiver.addCredit(AmqpMessageListenerContainer.this.initialCredits);
			}
			catch (ClientException ex) {
				throw ProtonUtils.toAmqpException(ex);
			}
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void close() {
			this.running = false;
			this.receiver.close();
		}

	}

}
