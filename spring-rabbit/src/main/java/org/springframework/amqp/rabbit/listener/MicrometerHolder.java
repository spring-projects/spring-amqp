/*
 * Copyright 2022-2025 the original author or authors.
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Builder;
import io.micrometer.core.instrument.Timer.Sample;

import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.lang.Nullable;

/**
 * Abstraction to avoid hard reference to Micrometer.
 *
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 2.4.6
 *
 */
public final class MicrometerHolder {

	private final ConcurrentMap<String, Timer> timers = new ConcurrentHashMap<>();

	private final MeterRegistry registry;

	private final Map<String, String> tags;

	private final String listenerId;

	MicrometerHolder(@Nullable ApplicationContext context, String listenerId, Map<String, String> tags) {
		if (context == null) {
			throw new IllegalStateException("No micrometer registry present");
		}
		try {
			this.registry = context.getBeanProvider(MeterRegistry.class).getIfUnique();
		}
		catch (NoUniqueBeanDefinitionException ex) {
			throw new IllegalStateException(ex);
		}
		if (this.registry != null) {
			this.listenerId = listenerId;
			this.tags = tags;
		}
		else {
			throw new IllegalStateException("No micrometer registry present (or more than one and "
					+ "there is not exactly one marked with @Primary)");
		}
	}

	public Object start() {
		return Timer.start(this.registry);
	}

	public void success(Object sample, String queue) {
		Timer timer = this.timers.get(queue + "none");
		if (timer == null) {
			timer = buildTimer(this.listenerId, "success", queue, "none");
		}
		((Sample) sample).stop(timer);
	}

	public void failure(Object sample, String queue, String exception) {
		Timer timer = this.timers.get(queue + exception);
		if (timer == null) {
			timer = buildTimer(this.listenerId, "failure", queue, exception);
		}
		((Sample) sample).stop(timer);
	}

	private Timer buildTimer(String aListenerId, String result, String queue, String exception) {

		Builder builder = Timer.builder("spring.rabbitmq.listener")
				.description("Spring RabbitMQ Listener")
				.tag("listener.id", aListenerId)
				.tag("queue", queue)
				.tag("result", result)
				.tag("exception", exception);
		if (this.tags != null && !this.tags.isEmpty()) {
			this.tags.forEach(builder::tag);
		}
		Timer registeredTimer = builder.register(this.registry);
		this.timers.put(queue + exception, registeredTimer);
		return registeredTimer;
	}

	void destroy() {
		this.timers.values().forEach(this.registry::remove);
		this.timers.clear();
	}

}
