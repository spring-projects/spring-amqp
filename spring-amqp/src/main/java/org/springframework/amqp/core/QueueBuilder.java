/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.amqp.core;

import java.util.Map;

/**
 * Builds a Spring AMQP Queue using a fluent API.
 *
 * @author Maciej Walkowiak
 * @author Gary Russell
 *
 * @since 1.6
 *
 */
public final class QueueBuilder extends AbstractBuilder {

	private static final NamingStrategy namingStrategy = Base64UrlNamingStrategy.DEFAULT; // NOSONAR lower case

	private final String name;

	private boolean durable;

	private boolean exclusive;

	private boolean autoDelete;

	/**
	 * Creates a builder for a durable queue with a generated
	 * unique name - {@code spring.gen-<random>}.
	 * @return the QueueBuilder instance.
	 */
	public static QueueBuilder durable() {
		return durable(namingStrategy.generateName());
	}

	/**
	 * Creates a builder for a non-durable (transient) queue.
	 * @return the QueueBuilder instance.
	 */
	public static QueueBuilder nonDurable() {
		return new QueueBuilder(namingStrategy.generateName());
	}

	/**
	 * Creates a builder for a durable queue.
	 * @param name the name of the queue.
	 * @return the QueueBuilder instance.
	 */
	public static QueueBuilder durable(String name) {
		return new QueueBuilder(name).setDurable();
	}

	/**
	 * Creates a builder for a non-durable (transient) queue.
	 * @param name the name of the queue.
	 * @return the QueueBuilder instance.
	 */
	public static QueueBuilder nonDurable(final String name) {
		return new QueueBuilder(name);
	}

	private QueueBuilder(String name) {
		this.name = name;
	}

	private QueueBuilder setDurable() {
		this.durable = true;
		return this;
	}

	/**
	 * The final queue will be exclusive.
	 * @return the QueueBuilder instance.
	 */
	public QueueBuilder exclusive() {
		this.exclusive = true;
		return this;
	}

	/**
	 * The final queue will auto delete.
	 * @return the QueueBuilder instance.
	 */
	public QueueBuilder autoDelete() {
		this.autoDelete = true;
		return this;
	}

	/**
	 * The final queue will contain argument used to declare a queue.
	 * @param key argument name
	 * @param value argument value
	 * @return the QueueBuilder instance.
	 */
	public QueueBuilder withArgument(String key, Object value) {
		getOrCreateArguments().put(key, value);
		return this;
	}

	/**
	 * The final queue will contain arguments used to declare a queue.
	 * @param arguments the arguments map
	 * @return the QueueBuilder instance.
	 */
	public QueueBuilder withArguments(Map<String, Object> arguments) {
		getOrCreateArguments().putAll(arguments);
		return this;
	}

	/**
	 * Set the message time-to-live after which it will be discarded, or routed to the
	 * dead-letter-exchange, if so configured.
	 * @param ttl the time to live (milliseconds).
	 * @return the builder.
	 * @since 2.2
	 * @see #deadLetterExchange(String)
	 */
	public QueueBuilder ttl(int ttl) {
		return withArgument("x-message-ttl", ttl);
	}

	/**
	 * Set the time that the queue can remain unused before being deleted.
	 * @param expires the expiration (milliseconds).
	 * @return the builder.
	 * @since 2.2
	 */
	public QueueBuilder expires(int expires) {
		return withArgument("x-expires", expires);
	}

	/**
	 * Set the number of (ready) messages allowed in the queue before it starts to drop
	 * them.
	 * @param count the number of (ready) messages allowed.
	 * @return the builder.
	 * @since 3.1
	 * @see #overflow(Overflow)
	 */
	public QueueBuilder maxLength(long count) {
		return withArgument("x-max-length", count);
	}

	/**
	 * Set the total aggregate body size allowed in the queue before it starts to drop
	 * them.
	 * @param bytes the total aggregate body size.
	 * @return the builder.
	 * @since 2.2
	 */
	public QueueBuilder maxLengthBytes(int bytes) {
		return withArgument("x-max-length-bytes", bytes);
	}

	/**
	 * Set the overflow mode when messages are dropped due to max messages or max message
	 * size is exceeded.
	 * @param overflow {@link Overflow#dropHead} or {@link Overflow#rejectPublish}.
	 * @return the builder.
	 * @since 2.2
	 */
	public QueueBuilder overflow(Overflow overflow) {
		return withArgument("x-overflow", overflow.getValue());
	}

	/**
	 * Set the dead-letter exchange to which to route expired or rejected messages.
	 * @param dlx the dead-letter exchange.
	 * @return the builder.
	 * @since 2.2
	 * @see #deadLetterRoutingKey(String)
	 */
	public QueueBuilder deadLetterExchange(String dlx) {
		return withArgument("x-dead-letter-exchange", dlx);
	}

	/**
	 * Set the routing key to use when routing expired or rejected messages to the
	 * dead-letter exchange.
	 * @param dlrk the dead-letter routing key.
	 * @return the builder.
	 * @since 2.2
	 * @see #deadLetterExchange(String)
	 */
	public QueueBuilder deadLetterRoutingKey(String dlrk) {
		return withArgument("x-dead-letter-routing-key", dlrk);
	}

	/**
	 * Set the maximum number if priority levels for the queue to support; if not set, the
	 * queue will not support message priorities.
	 * @param maxPriority the maximum priority.
	 * @return the builder.
	 * @since 2.2
	 */
	public QueueBuilder maxPriority(int maxPriority) {
		return withArgument("x-max-priority", maxPriority);
	}

	/**
	 * Set the queue into lazy mode, keeping as many messages as possible on disk to
	 * reduce RAM usage on the broker. if not set, the queue will keep an in-memory cache
	 * to deliver messages as fast as possible.
	 * @return the builder.
	 * @since 2.2
	 */
	public QueueBuilder lazy() {
		return withArgument("x-queue-mode", "lazy");
	}

	/**
	 * Set the master locator mode which determines which node a queue master will be
	 * located on a cluster of nodes.
	 * @param locator {@link LeaderLocator}.
	 * @return the builder.
	 * @since 2.2
	 */
	public QueueBuilder leaderLocator(LeaderLocator locator) {
		return withArgument("x-queue-master-locator", locator.getValue());
	}

	/**
	 * Set the 'x-single-active-consumer' queue argument.
	 * @return the builder.
	 * @since 2.2.2
	 */
	public QueueBuilder singleActiveConsumer() {
		return withArgument("x-single-active-consumer", true);
	}

	/**
	 * Set the queue argument to declare a queue of type 'classic' instead of default type.
	 * @return the builder.
	 * @since 2.2.2
	 */
	public QueueBuilder classic() {
		return withArgument("x-queue-type", "classic");
	}
	
	/**
	 * Set the queue argument to declare a queue of type 'quorum' instead of 'classic'.
	 * @return the builder.
	 * @since 2.2.2
	 */
	public QueueBuilder quorum() {
		return withArgument("x-queue-type", "quorum");
	}

	/**
	 * Set the queue argument to declare a queue of type 'stream' instead of 'classic'.
	 * @return the builder.
	 * @since 2.4
	 */
	public QueueBuilder stream() {
		return withArgument("x-queue-type", "stream");
	}

	/**
	 * Set the delivery limit; only applies to quorum queues.
	 * @param limit the limit.
	 * @return the builder.
	 * @since 2.2.2
	 * @see #quorum()
	 */
	public QueueBuilder deliveryLimit(int limit) {
		return withArgument("x-delivery-limit", limit);
	}

	/**
	 * Builds a final queue.
	 * @return the Queue instance.
	 */
	public Queue build() {
		return new Queue(this.name, this.durable, this.exclusive, this.autoDelete, getArguments());
	}

	/**
	 * Overflow argument values.
	 */
	public enum Overflow {

		/**
		 * Drop the oldest message.
		 */
		dropHead("drop-head"),

		/**
		 * Reject the new message.
		 */
		rejectPublish("reject-publish");

		private final String value;

		Overflow(String value) {
			this.value = value;
		}

		/**
		 * Return the value.
		 * @return the value.
		 */
		public String getValue() {
			return this.value;
		}

	}

	/**
	 * Locate the queue leader.
	 *
	 * @since 2.3.7
	 *
	 */
	public enum LeaderLocator {

		/**
		 * Deploy on the node with the fewest queue leaders.
		 */
		minLeaders("min-masters"),

		/**
		 * Deploy on the node we are connected to.
		 */
		clientLocal("client-local"),

		/**
		 * Deploy on a random node.
		 */
		random("random");

		private final String value;

		LeaderLocator(String value) {
			this.value = value;
		}

		/**
		 * Return the value.
		 * @return the value.
		 */
		public String getValue() {
			return this.value;
		}

	}

}
