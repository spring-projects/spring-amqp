/*
 * Copyright 2002-2025 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

/**
 * Base class for {@link Declarable} classes.
 *
 * @author Gary Russell
 * @author Christian Tzolov
 * @author Ngoc Nhan
 * @since 1.2
 *
 */
public abstract class AbstractDeclarable implements Declarable {

	private final Lock lock = new ReentrantLock();

	protected final Map<String, Object> arguments;

	private boolean shouldDeclare = true;

	private boolean ignoreDeclarationExceptions;

	private Collection<Object> declaringAdmins = new ArrayList<>();

	public AbstractDeclarable() {
		this(null);
	}

	/**
	 * Construct an instance with the supplied arguments, or an empty map if null.
	 * @param arguments the arguments.
	 * @since 2.2.2
	 */
	public AbstractDeclarable(@Nullable Map<String, @Nullable Object> arguments) {
		if (arguments != null) {
			this.arguments = new HashMap<>(arguments);
		}
		else {
			this.arguments = new HashMap<>();
		}
	}

	@Override
	public boolean shouldDeclare() {
		return this.shouldDeclare;
	}

	/**
	 * Whether this object should be automatically declared
	 * by any {@code AmqpAdmin}. Default is {@code true}.
	 * @param shouldDeclare true or false.
	 */
	public void setShouldDeclare(boolean shouldDeclare) {
		this.shouldDeclare = shouldDeclare;
	}

	@Override
	public Collection<?> getDeclaringAdmins() {
		return Collections.unmodifiableCollection(this.declaringAdmins);
	}

	@Override
	public boolean isIgnoreDeclarationExceptions() {
		return this.ignoreDeclarationExceptions;
	}

	/**
	 * Set to true to ignore exceptions such as mismatched properties when declaring.
	 * @param ignoreDeclarationExceptions the ignoreDeclarationExceptions.
	 * @since 1.6
	 */
	public void setIgnoreDeclarationExceptions(boolean ignoreDeclarationExceptions) {
		this.ignoreDeclarationExceptions = ignoreDeclarationExceptions;
	}

	@Override
	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	public void setAdminsThatShouldDeclare(@Nullable Object @Nullable ... adminArgs) {
		Collection<Object> admins = new ArrayList<>();
		if (adminArgs != null) {
			if (adminArgs.length > 1) {
				Assert.noNullElements(adminArgs, "'admins' cannot contain null elements");
			}
			if (adminArgs.length > 0 && !(adminArgs.length == 1 && adminArgs[0] == null)) {
				admins = Arrays.asList(adminArgs);
			}
		}
		this.declaringAdmins = admins;
	}

	@Override
	public void addArgument(String argName, Object argValue) {
		this.lock.lock();
		try {
			this.arguments.put(argName, argValue);
		}
		finally {
			this.lock.unlock();
		}
	}

	@Override
	public Object removeArgument(String name) {
		this.lock.lock();
		try {
			return this.arguments.remove(name);
		}
		finally {
			this.lock.unlock();
		}
	}

	public Map<String, Object> getArguments() {
		return this.arguments;
	}

}
