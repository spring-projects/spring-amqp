/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.core;

import java.util.Map;

/**
 * Builds a Spring AMQP Queue using a fluent API.
 *
 * @author Maciej Walkowiak
 * @since 1.6
 *
 */
public final class QueueBuilder extends AbstractBuilder {

	private static final AnonymousQueue.NamingStrategy namingStrategy = new AnonymousQueue.Base64UrlNamingStrategy();

	private final String name;

	private boolean durable;

	private boolean exclusive;

	private boolean autoDelete;

	/**
	 * Creates a builder for a durable queue with a generated
	 * unique name - spring.gen-<random>.
	 *
	 * @param name the name of the queue.
	 * @return The Builder.
	 */
	public static QueueBuilder durable() {
		return durable(namingStrategy.generateName());
	}

	/**
	 * Creates a builder for a non-durable (transient) queue.
	 *
	 * @param name the name of the queue.
	 * @return The Builder.
	 */
	public static QueueBuilder nonDurable() {
		return new QueueBuilder(namingStrategy.generateName());
	}

	/**
	 * Creates a builder for a durable queue.
	 *
	 * @param name the name of the queue.
	 * @return The Builder.
	 */
	public static QueueBuilder durable(String name) {
		return new QueueBuilder(name).setDurable();
	}

	/**
	 * Creates a builder for a non-durable (transient) queue.
	 *
	 * @param name the name of the queue.
	 * @return The Builder.
	 */
	public static QueueBuilder nonDurable(final String name) {
		return new QueueBuilder(name);
	}

	private QueueBuilder(String name) {
		this.name = name;
	}

	/**
	 * The final queue will be durable.
	 * @return The Builder.
	 */
	private QueueBuilder setDurable() {
		this.durable = true;
		return this;
	}

	/**
	 * The final queue will be exclusive.
	 * @return The Builder.
	 */
	public QueueBuilder exclusive() {
		this.exclusive = true;
		return this;
	}

	/**
	 * The final queue will auto delete.
	 * @return The Builder.
	 */
	public QueueBuilder autoDelete() {
		this.autoDelete = true;
		return this;
	}

	/**
	 * The final queue will contain argument used to declare a queue.
	 *
	 * @param key argument name
	 * @param value argument value
	 * @return The Builder.
	 */
	public QueueBuilder withArgument(String key, Object value) {
		this.getOrCreateArguments().put(key, value);
		return this;
	}

	/**
	 * The final queue will contain arguments used to declare a queue.
	 *
	 * @param arguments arguments map
	 * @return The Builder.
	 */
	public QueueBuilder withArguments(Map<String, Object> arguments) {
		this.getOrCreateArguments().putAll(arguments);
		return this;
	}

	/**
	 * Builds a final queue.
	 * @return The Queue.
	 */
	public Queue build() {
		return new Queue(this.name, this.durable, this.exclusive, this.autoDelete, getArguments());
	}

}
