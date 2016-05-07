/*
 * Copyright 2002-2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.springframework.util.Assert;

/**
 * Base class for {@link Declarable} classes.
 *
 * @author Gary Russell
 * @since 1.2
 *
 */
public abstract class AbstractDeclarable implements Declarable {

	private volatile boolean shouldDeclare = true;

	private volatile Collection<Object> declaringAdmins = new ArrayList<Object>();

	private boolean ignoreDeclarationExceptions;

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

	/**
	 * The {@code AmqpAdmin}s that should declare this object; default is
	 * all admins.
	 * <br><br>A null argument, or an array/varArg with a single null argument, clears the collection
	 * ({@code setAdminsThatShouldDeclare((AmqpAdmin) null)} or
	 * {@code setAdminsThatShouldDeclare((AmqpAdmin[]) null)}). Clearing the collection resets
	 * the behavior such that all admins will declare the object.
	 * @param admins The admins.
	 */
	public void setAdminsThatShouldDeclare(Object... admins) {
		Collection<Object> declaringAdmins = new ArrayList<Object>();
		if (admins != null) {
			if (admins.length > 1) {
				Assert.noNullElements(admins, "'admins' cannot contain null elements");
			}
			if (admins.length > 0 && !(admins.length == 1 && admins[0] == null)) {
				declaringAdmins.addAll(Arrays.asList(admins));
			}
		}
		this.declaringAdmins = declaringAdmins;
	}

}
