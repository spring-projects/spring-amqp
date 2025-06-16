/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.rabbit.stream.support;

import java.util.function.Consumer;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.StreamCreator;

import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

/**
 * Used to provision streams.
 *
 * @author Gary Russell
 * @since 2.4.13
 *
 */
public class StreamAdmin implements SmartLifecycle {

	private final StreamCreator streamCreator;

	private final Consumer<StreamCreator> callback;

	private boolean autoStartup = true;

	private int phase;

	private volatile boolean running;

	/**
	 * Construct with the provided parameters.
	 * @param env the environment.
	 * @param callback the callback to receive the {@link StreamCreator}.
	 */
	public StreamAdmin(Environment env, Consumer<StreamCreator> callback) {
		Assert.notNull(env, "Environment cannot be null");
		Assert.notNull(callback, "'callback' cannot be null");
		this.streamCreator = env.streamCreator();
		this.callback = callback;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	/**
	 * Set the phase; default is 0.
	 * @param phase the phase.
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * Set to false to prevent automatic startup.
	 * @param autoStartup the autoStartup.
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public void start() {
		this.callback.accept(this.streamCreator);
		this.running = true;
	}

	@Override
	public void stop() {
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

}
