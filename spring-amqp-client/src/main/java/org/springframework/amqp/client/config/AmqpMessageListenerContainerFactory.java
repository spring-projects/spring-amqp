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

package org.springframework.amqp.client.config;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executor;

import org.aopalliance.aop.Advice;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.client.AmqpConnectionFactory;
import org.springframework.amqp.client.listener.AmqpMessageListenerContainer;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.util.ErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * The configuration factory to produce {@link AmqpMessageListenerContainer} instance
 * based on the provided {@link AmqpListenerEndpoint}.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see AmqpMessageListenerContainer
 */
public class AmqpMessageListenerContainerFactory {

	private final AmqpConnectionFactory connectionFactory;

	private @Nullable ErrorHandler errorHandler;

	private @Nullable Integer concurrency;

	private @Nullable Boolean autoStartup;

	private @Nullable Boolean autoAccept;

	private @Nullable Executor taskExecutor;

	private @Nullable Integer initialCredits;

	private @Nullable Duration receiveTimeout;

	private @Nullable Duration gracefulShutdownPeriod;

	private @Nullable BackOff recoveryBackOff;

	private Advice @Nullable [] adviceChain;

	/**
	 * Create an instance with the provided {@link AmqpConnectionFactory}.
	 * @param connectionFactory the connection factory to use.
	 */
	public AmqpMessageListenerContainerFactory(AmqpConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * The default concurrency for container instances created by this factory.
	 * Can be overridden by {@link AmqpListenerEndpoint#getConcurrency()}.
	 * Each AMQP address runs in its own consumer; set this property to create multiple
	 * consumers for each AMQP address.
	 * @param concurrency the consumers per AMQP address.
	 * @see AmqpMessageListenerContainer#setConsumersPerQueue(int)
	 */
	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	/**
	 * The default auto startup flag for container instances created by this factory.
	 * Can be overridden by  {@link AmqpListenerEndpoint#getAutoStartup()}.
	 * @param autoStartup the auto startup flag.
	 * @see AmqpMessageListenerContainer#setAutoStartup(boolean)
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * The default auto accept flag for container instances created by this factory.
	 * Can be overridden by {@link AmqpListenerEndpoint#getAutoAccept()}.
	 * @param autoAccept flag to use.
	 * @see AmqpMessageListenerContainer#setAutoAccept(boolean)
	 */
	public void setAutoAccept(boolean autoAccept) {
		this.autoAccept = autoAccept;
	}

	/**
	 * The default {@link Executor} for container instances created by this factory.
	 * Can be overridden by {@link AmqpListenerEndpoint#getTaskExecutor()}.
	 * @param taskExecutor the task executor.
	 * @see AmqpMessageListenerContainer#setTaskExecutor(Executor)
	 */
	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * The default initial credits value for container instances created by this factory.
	 * Can be overridden by {@link AmqpListenerEndpoint#getInitialCredits()}.
	 * @param initialCredits the initial credits.
	 * @see AmqpMessageListenerContainer#setInitialCredits(int)
	 */
	public void setInitialCredits(int initialCredits) {
		this.initialCredits = initialCredits;
	}

	/**
	 * The default {@code receiveTimeout} for container instances created by this factory.
	 * Can be overridden by {@link AmqpListenerEndpoint#getReceiveTimeout()}.
	 * @param receiveTimeout the timeout for receiving messages.
	 * @see AmqpMessageListenerContainer#setReceiveTimeout(Duration)
	 */
	public void setReceiveTimeout(Duration receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	/**
	 * The default {@code gracefulShutdownPeriod} for container instances created by this factory.
	 * Can be overridden by {@link AmqpListenerEndpoint#getGracefulShutdownPeriod()}.
	 * @param gracefulShutdownPeriod the timeout for shutdown.
	 * @see AmqpMessageListenerContainer#setGracefulShutdownPeriod(Duration)
	 */
	public void setGracefulShutdownPeriod(Duration gracefulShutdownPeriod) {
		this.gracefulShutdownPeriod = gracefulShutdownPeriod;
	}

	/**
	 * The default recovery interval for container instances created by this factory.
	 * @param recoveryInterval the interval between consumer recovery attempts.
	 * @since 4.1.1
	 * @see AmqpMessageListenerContainer#setRecoveryInterval(Duration)
	 */
	public void setRecoveryInterval(Duration recoveryInterval) {
		setRecoveryBackOff(new FixedBackOff(recoveryInterval.toMillis(), FixedBackOff.UNLIMITED_ATTEMPTS));
	}

	/**
	 * The default {@link BackOff} for a failed consumer recovery
	 * in container instances created by this factory.
	 * @param recoveryBackOff the {@link BackOff} to use for the consumer recovery.
	 * @since 4.1.1
	 * @see AmqpMessageListenerContainer#setRecoveryBackOff(BackOff)
	 */
	public void setRecoveryBackOff(BackOff recoveryBackOff) {
		this.recoveryBackOff = recoveryBackOff;
	}

	/**
	 * Set The default advice chain for container instances created by this factory.
	 * Can be overridden by {@link AmqpListenerEndpoint#getAdviceChain()}.
	 * @param advices the advice chain.
	 */
	public void setAdviceChain(Advice... advices) {
		this.adviceChain = Arrays.copyOf(advices, advices.length);
	}

	/**
	 * The {@link ErrorHandler} for container instances created by this factory.
	 * @param errorHandler the error handler.
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Create a {@link AmqpMessageListenerContainer} instance based on the provided {@link AmqpListenerEndpoint}.
	 * @param listenerEndpoint the endpoint to configure a {@link AmqpMessageListenerContainer} instance.
	 * @return the container instance.
	 */
	public AmqpMessageListenerContainer createContainer(AmqpListenerEndpoint listenerEndpoint) {
		configureEndpoint(listenerEndpoint);
		AmqpMessageListenerContainer listenerContainer = new AmqpMessageListenerContainer(this.connectionFactory);
		listenerContainer.setQueueNames(listenerEndpoint.getAddresses());
		JavaUtils.INSTANCE
				.acceptIfNotNull(this.errorHandler, listenerContainer::setErrorHandler)
				.acceptIfNotNull(this.recoveryBackOff, listenerContainer::setRecoveryBackOff)
				.acceptIfNotNull(listenerEndpoint.getId(), listenerContainer::setBeanName)
				.acceptOrElseIfNotNull(
						listenerEndpoint.getConcurrency(), this.concurrency,
						listenerContainer::setConsumersPerQueue)
				.acceptOrElseIfNotNull(
						listenerEndpoint.getAutoStartup(), this.autoStartup,
						listenerContainer::setAutoStartup)
				.acceptOrElseIfNotNull(
						listenerEndpoint.getAutoAccept(), this.autoAccept,
						listenerContainer::setAutoAccept)
				.acceptOrElseIfNotNull(
						listenerEndpoint.getTaskExecutor(), this.taskExecutor,
						listenerContainer::setTaskExecutor)
				.acceptOrElseIfNotNull(
						listenerEndpoint.getInitialCredits(), this.initialCredits,
						listenerContainer::setInitialCredits)
				.acceptOrElseIfNotNull(
						listenerEndpoint.getReceiveTimeout(), this.receiveTimeout,
						listenerContainer::setReceiveTimeout)
				.acceptOrElseIfNotNull(
						listenerEndpoint.getGracefulShutdownPeriod(), this.gracefulShutdownPeriod,
						listenerContainer::setGracefulShutdownPeriod)
				.acceptOrElseIfNotNull(
						listenerEndpoint.getAdviceChain(), this.adviceChain,
						listenerContainer::setAdviceChain);
		listenerContainer.setupMessageListener(listenerEndpoint.getMessageListener());
		return listenerContainer;
	}

	protected void configureEndpoint(AmqpListenerEndpoint endpoint) {

	}

}
