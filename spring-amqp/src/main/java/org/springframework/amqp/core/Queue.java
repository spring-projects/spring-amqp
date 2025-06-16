/*
 * Copyright 2002-present the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Simple container collecting information to describe a queue. Used in conjunction with AmqpAdmin.
 *
 * @author Mark Pollack
 * @author Gary Russell
 *
 * @see AmqpAdmin
 */
public class Queue extends AbstractDeclarable implements Cloneable {

	/**
	 * Argument key for the queue leader locator.
	 * @since 2.1
	 */
	public static final String X_QUEUE_LEADER_LOCATOR = "x-queue-master-locator";

	private final String name;

	private final boolean durable;

	private final boolean exclusive;

	private final boolean autoDelete;

	private volatile String actualName;

	/**
	 * The queue is durable, non-exclusive and non auto-delete.
	 *
	 * @param name the name of the queue.
	 */
	public Queue(String name) {
		this(name, true, false, false);
	}

	/**
	 * Construct a new queue, given a name and durability flag. The queue is non-exclusive and non auto-delete.
	 *
	 * @param name the name of the queue.
	 * @param durable true if we are declaring a durable queue (the queue will survive a server restart)
	 */
	public Queue(String name, boolean durable) {
		this(name, durable, false, false, null);
	}

	/**
	 * Construct a new queue, given a name, durability, exclusive and auto-delete flags.
	 * @param name the name of the queue.
	 * @param durable true if we are declaring a durable queue (the queue will survive a server restart)
	 * @param exclusive true if we are declaring an exclusive queue (the queue will only be used by the declarer's
	 * connection)
	 * @param autoDelete true if the server should delete the queue when it is no longer in use
	 */
	public Queue(String name, boolean durable, boolean exclusive, boolean autoDelete) {
		this(name, durable, exclusive, autoDelete, null);
	}

	/**
	 * Construct a new queue, given a name, durability flag, and auto-delete flag, and arguments.
	 * @param name the name of the queue - must not be null; set to "" to have the broker generate the name.
	 * @param durable true if we are declaring a durable queue (the queue will survive a server restart)
	 * @param exclusive true if we are declaring an exclusive queue (the queue will only be used by the declarer's
	 * connection)
	 * @param autoDelete true if the server should delete the queue when it is no longer in use
	 * @param arguments the arguments used to declare the queue
	 */
	public Queue(String name, boolean durable, boolean exclusive, boolean autoDelete,
			@Nullable Map<String, @Nullable Object> arguments) {

		super(arguments);
		Assert.notNull(name, "'name' cannot be null");
		this.name = name;
		this.actualName = StringUtils.hasText(name) ? name
				: (Base64UrlNamingStrategy.DEFAULT.generateName() + "_awaiting_declaration");
		this.durable = durable;
		this.exclusive = exclusive;
		this.autoDelete = autoDelete;
	}

	/**
	 * Return the name provided in the constructor.
	 * @return the name.
	 * @see #getActualName()
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * A durable queue will survive a server restart.
	 *
	 * @return true if durable.
	 */
	public boolean isDurable() {
		return this.durable;
	}

	/**
	 * True if the server should only send messages to the declarer's connection.
	 *
	 * @return true if exclusive.
	 */
	public boolean isExclusive() {
		return this.exclusive;
	}

	/**
	 * True if the server should delete the queue when it is no longer in use (the last consumer is cancelled). A queue
	 * that never has any consumers will not be deleted automatically.
	 *
	 * @return true if auto-delete.
	 */
	public boolean isAutoDelete() {
		return this.autoDelete;
	}

	/**
	 * Set the name from the DeclareOk.
	 * @param name the name.
	 * @since 2.1
	 */
	public void setActualName(String name) {
		this.actualName = name;
	}

	/**
	 * Return the name provided to the constructor or the broker-generated name
	 * if that name is an empty String.
	 * @return the name.
	 * @since 2.1
	 */
	public String getActualName() {
		return this.actualName;
	}

	/**
	 * Set the leader locator strategy argument for this queue.
	 * @param locator the locator; null to clear the argument.
	 * @since 2.3.7
	 */
	public final void setLeaderLocator(@Nullable String locator) {
		if (locator == null) {
			removeArgument(X_QUEUE_LEADER_LOCATOR);
		}
		else {
			addArgument(X_QUEUE_LEADER_LOCATOR, locator);
		}
	}

	@Override
	public Object clone() {  // NOSONAR - doesn't throw CloneNotSupportedException
		Queue queue = new Queue(this.name, this.durable, this.exclusive, // NOSONAR - doesn't need to call super.clone()
				this.autoDelete, new HashMap<>(getArguments()));
		queue.setActualName(this.actualName);
		return queue;
	}

	@Override
	public String toString() {
		return "Queue [name=" + this.name + ", durable=" + this.durable + ", autoDelete=" + this.autoDelete
				+ ", exclusive=" + this.exclusive + ", arguments=" + getArguments()
				+ ", actualName=" + this.actualName + "]";
	}

}
