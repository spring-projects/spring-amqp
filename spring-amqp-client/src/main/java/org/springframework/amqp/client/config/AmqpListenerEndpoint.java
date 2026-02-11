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
import java.util.concurrent.Executor;

import org.aopalliance.aop.Advice;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageListener;

/**
 * The configuration model to represent a {@link MessageListener}
 * with properties for target {@link org.springframework.amqp.client.listener.AmqpMessageListenerContainer}
 * to be registered as a bean in the application context.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see AmqpMessageListenerContainerFactory
 * @see org.springframework.amqp.client.listener.AmqpMessageListenerContainer
 */
public interface AmqpListenerEndpoint {

	/**
	 * Return the message listener to be used in the target listener container bean.
	 * @return the message listener.
	 */
	MessageListener getMessageListener();

	/**
	 * Return the AMQP addresses to listen to.
	 * @return the addresses.
	 */
	String[] getAddresses();

	/**
	 * Return the id of the target listener container bean.
	 * TODO: If not provided, the {@link AmqpMessageListenerContainerFactory} makes a decision about generated bean name.
	 * @return the id
	 */
	@Nullable
	String getId();

	/**
	 * Return the concurrency (consumers per AMQP address) for the target listener container bean.
	 * The {@link AmqpMessageListenerContainerFactory} configuration is used by default.
	 * @return the concurrency
	 */
	@Nullable
	Integer getConcurrency();

	/**
	 * Override of the default {@code autoStartup} property from the {@link AmqpMessageListenerContainerFactory}.
	 * @return the autoStartup.
	 */
	@Nullable
	Boolean getAutoStartup();

	/**
	 * Get the task executor for the target listener container bean.
	 * Overrides any executor set on the {@link AmqpMessageListenerContainerFactory}.
	 * @return the executor.
	 */
	@Nullable
	Executor getTaskExecutor();

	/**
	 * Override the {@code autoAccept} property in the {@link AmqpMessageListenerContainerFactory}.
	 * @return true if auto-accept is enabled.
	 */
	@Nullable
	Boolean getAutoAccept();

	/**
	 * The initial number of credits to grant to the AMQP receiver.
	 * Override the {@code initialCredits} property in the {@link AmqpMessageListenerContainerFactory}.
	 * @return number of initial credits
	 */
	@Nullable
	Integer getInitialCredits();

	/**
	 * The {@code receiveTimeout} for the target listener container bean.
	 * @return the timeout for receiving messages.
	 */
	@Nullable
	Duration getReceiveTimeout();

	/**
	 * The graceful shutdown period for the target listener container bean.
	 * @return the graceful shutdown period.
	 */
	@Nullable
	Duration getGracefulShutdownPeriod();

	/**
	 * The advice chain for the target listener container bean.
	 * @return the advice chain.
	 */
	Advice @Nullable [] getAdviceChain();

}
