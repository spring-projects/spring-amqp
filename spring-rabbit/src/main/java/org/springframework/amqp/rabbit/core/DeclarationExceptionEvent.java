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

package org.springframework.amqp.rabbit.core;

import org.springframework.amqp.core.Declarable;

/**
 * Application event published when a declaration exception occurs.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class DeclarationExceptionEvent extends RabbitAdminEvent {

	private static final long serialVersionUID = -8367796410619780665L;

	private final Declarable declarable;

	private final Throwable throwable;

	public DeclarationExceptionEvent(Object source, Declarable declarable, Throwable t) {
		super(source);
		this.declarable = declarable;
		this.throwable = t;
	}

	/**
	 * @return the declarable - if null, we were declaring a broker-named queue.
	 */
	public Declarable getDeclarable() {
		return this.declarable;
	}

	/**
	 * @return the throwable.
	 */
	public Throwable getThrowable() {
		return this.throwable;
	}

	@Override
	public String toString() {
		return "DeclarationExceptionEvent [declarable=" + this.declarable + ", throwable=" + this.throwable + ", source="
				+ getSource() + "]";
	}

}
