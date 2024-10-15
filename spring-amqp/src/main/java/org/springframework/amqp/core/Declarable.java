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

import java.util.Collection;

import org.springframework.lang.Nullable;

/**
 * Classes implementing this interface can be auto-declared
 * with the broker during context initialization by an {@code AmqpAdmin}.
 * Registration can be limited to specific {@code AmqpAdmin}s.
 *
 * @author Gary Russell
 * @since 1.2
 *
 */
public interface Declarable {

	/**
	 * Whether or not this object should be automatically declared
	 * by any {@code AmqpAdmin}.
	 * @return true if the object should be declared.
	 */
	boolean shouldDeclare();

	/**
	 * The collection of {@code AmqpAdmin}s that should declare this
	 * object; if empty, all admins should declare.
	 * @return the collection.
	 */
	Collection<?> getDeclaringAdmins();

	/**
	 * Should ignore exceptions (such as mismatched args) when declaring.
	 * @return true if should ignore.
	 * @since 1.6
	 */
	boolean isIgnoreDeclarationExceptions();

	/**
	 * The {@code AmqpAdmin}s that should declare this object; default is
	 * all admins.
	 * <br><br>A null argument, or an array/varArg with a single null argument, clears the collection
	 * ({@code setAdminsThatShouldDeclare((AmqpAdmin) null)} or
	 * {@code setAdminsThatShouldDeclare((AmqpAdmin[]) null)}). Clearing the collection resets
	 * the behavior such that all admins will declare the object.
	 * @param adminArgs The admins.
	 */
	void setAdminsThatShouldDeclare(@Nullable Object... adminArgs);

	/**
	 * Add an argument to the declarable.
	 * @param name the argument name.
	 * @param value the argument value.
	 * @since 2.2.2
	 */
	default void addArgument(String name, Object value) {
		// default no-op
	}

	/**
	 * Remove an argument from the declarable.
	 * @param name the argument name.
	 * @return the argument value or null if not present.
	 * @since 2.2.2
	 */
	default @Nullable Object removeArgument(String name) {
		return null;
	}

}
