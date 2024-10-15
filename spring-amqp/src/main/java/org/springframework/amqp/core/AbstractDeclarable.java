/*
 * Copyright 2002-2024 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

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

	private boolean shouldDeclare = true;

	private Collection<Object> declaringAdmins = new ArrayList<>();

	private boolean ignoreDeclarationExceptions;

	private final Map<String, Object> arguments;

	public AbstractDeclarable() {
		this(null);
	}

	/**
	 * Construct an instance with the supplied arguments, or an empty map if null.
	 * @param arguments the arguments.
	 * @since 2.2.2
	 */
	public AbstractDeclarable(@Nullable Map<String, Object> arguments) {
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
	 * Whether or not this object should be automatically declared
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
	public void setAdminsThatShouldDeclare(Object... adminArgs) {
		this.declaringAdmins = new ArrayList<>();
		if (ObjectUtils.isEmpty(adminArgs)) {
			return;
		}

		if (adminArgs.length > 1) {
			Assert.noNullElements(adminArgs, "'admins' cannot contain null elements");
			this.declaringAdmins.addAll(Arrays.asList(adminArgs));
		}
		else if (adminArgs[0] != null) {
			this.declaringAdmins.add(adminArgs[0]);
		}
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
