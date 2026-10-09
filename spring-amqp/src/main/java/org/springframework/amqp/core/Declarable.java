/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.Collection;

import org.jspecify.annotations.Nullable;

/**
 * Classes implementing this interface can be auto-declared
 * with the broker during context initialization by an {@code AmqpAdmin}.
 * Registration can be limited to specific {@code AmqpAdmin}s.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Ngoc Nhan
 *
 * @since 1.2
 *
 */
public interface Declarable {

	/**
	 * Whether this object should be automatically declared by any {@code AmqpAdmin}.
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
	 * @return true if it should ignore.
	 * @since 1.6
	 */
	boolean isIgnoreDeclarationExceptions();

	/**
	 * The {@code AmqpAdmin}s that should declare this object; default is
	 * all admins.
	 * <p>
	 * A null argument, or an array/varArg with a single null argument, clears the collection
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
