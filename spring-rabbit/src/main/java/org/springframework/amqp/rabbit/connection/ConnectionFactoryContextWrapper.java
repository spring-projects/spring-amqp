/*
 * Copyright 2020 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import java.util.concurrent.Callable;

import org.springframework.util.StringUtils;

/**
 * Helper class to handle {@link ConnectionFactory} context binding and unbinding when executing instructions.
 *
 * @author Wander Costa
 */
public class ConnectionFactoryContextWrapper {

	private final ConnectionFactory connectionFactory;

	public ConnectionFactoryContextWrapper(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * Executes a {@link Callable} binding to the default {@link ConnectionFactory} and finally unbinding it.
	 *
	 * @param callable the {@link Callable} object to be executed.
	 * @param <T>      the return type.
	 * @return the result of the {@link Callable}.
	 * @throws Exception when an Exception is thrown by the {@link Callable}.
	 */
	public <T> T call(final Callable<T> callable) throws Exception {
		return call(null, callable);
	}

	/**
	 * Executes a {@link Callable} binding the given {@link ConnectionFactory} and finally unbinding it.
	 *
	 * @param contextName the name of the context. In null, empty or blank, default context is bound.
	 * @param callable    the {@link Callable} object to be executed.
	 * @param <T>         the return type.
	 * @return the result of the {@link Callable}.
	 * @throws Exception when an Exception is thrown by the {@link Callable}.
	 */
	public <T> T call(final String contextName, final Callable<T> callable) throws Exception {
		try {
			bind(contextName);
			return callable.call();
		}
		finally {
			unbind(contextName);
		}
	}

	/**
	 * Executes a {@link Runnable} binding to the default {@link ConnectionFactory} and finally unbinding it.
	 *
	 * @param runnable the {@link Runnable} object to be executed.
	 * @throws RuntimeException when a RuntimeException is thrown by the {@link Runnable}.
	 */
	public void run(final Runnable runnable) {
		run(null, runnable);
	}

	/**
	 * Executes a {@link Runnable} binding the given {@link ConnectionFactory} and finally unbinding it.
	 *
	 * @param contextName the name of the context. In null, empty or blank, default context is bound.
	 * @param runnable    the {@link Runnable} object to be executed.
	 * @throws RuntimeException when a RuntimeException is thrown by the {@link Runnable}.
	 */
	public void run(final String contextName, final Runnable runnable) {
		try {
			bind(contextName);
			runnable.run();
		}
		finally {
			unbind(contextName);
		}
	}

	/**
	 * Binds the context.
	 *
	 * @param contextName the name of the context for the connection factory.
	 */
	private void bind(final String contextName) {
		if (StringUtils.hasText(contextName)) {
			SimpleResourceHolder.bind(this.connectionFactory, contextName);
		}
	}

	/**
	 * Unbinds the context.
	 *
	 * @param contextName the name of the context for the connection factory.
	 */
	private void unbind(final String contextName) {
		if (StringUtils.hasText(contextName)) {
			SimpleResourceHolder.unbind(this.connectionFactory);
		}
	}

}
