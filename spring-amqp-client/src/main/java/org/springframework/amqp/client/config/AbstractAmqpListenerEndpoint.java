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

/**
 * The abstract {@link AmqpListenerEndpoint} implementation holding common properties.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public abstract class AbstractAmqpListenerEndpoint implements AmqpListenerEndpoint {

	private final String[] addresses;

	private @Nullable String id;

	private @Nullable Integer concurrency;

	private @Nullable Boolean autoStartup;

	private @Nullable Executor taskExecutor;

	private @Nullable Boolean autoAccept;

	private @Nullable Integer initialCredits;

	private @Nullable Duration receiveTimeout;

	private @Nullable Duration gracefulShutdownPeriod;

	private Advice @Nullable [] adviceChain;

	protected AbstractAmqpListenerEndpoint(String... addresses) {
		this.addresses = Arrays.copyOf(addresses, addresses.length);
	}

	@Override
	public String[] getAddresses() {
		return this.addresses;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public @Nullable String getId() {
		return this.id;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	@Override
	public @Nullable Integer getConcurrency() {
		return this.concurrency;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public @Nullable Boolean getAutoStartup() {
		return this.autoStartup;
	}

	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	@Override
	public @Nullable Executor getTaskExecutor() {
		return this.taskExecutor;
	}

	public void setAutoAccept(boolean autoAccept) {
		this.autoAccept = autoAccept;
	}

	@Override
	public @Nullable Boolean getAutoAccept() {
		return this.autoAccept;
	}

	public void setInitialCredits(int initialCredits) {
		this.initialCredits = initialCredits;
	}

	@Override
	public @Nullable Integer getInitialCredits() {
		return this.initialCredits;
	}

	public void setReceiveTimeout(Duration receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	@Override
	public @Nullable Duration getReceiveTimeout() {
		return this.receiveTimeout;
	}

	public void setGracefulShutdownPeriod(Duration gracefulShutdownPeriod) {
		this.gracefulShutdownPeriod = gracefulShutdownPeriod;
	}

	@Override
	public @Nullable Duration getGracefulShutdownPeriod() {
		return this.gracefulShutdownPeriod;
	}

	public void setAdviceChain(Advice... adviceChain) {
		this.adviceChain = Arrays.copyOf(adviceChain, adviceChain.length);
	}

	@Override
	public Advice @Nullable [] getAdviceChain() {
		return this.adviceChain;
	}

}
