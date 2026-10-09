/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import java.io.Serial;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Declarable;

/**
 * Application event published when a declaration exception occurs.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class DeclarationExceptionEvent extends RabbitAdminEvent {

	@Serial
	private static final long serialVersionUID = -8367796410619780665L;

	private final transient @Nullable Declarable declarable;

	private final Throwable throwable;

	public DeclarationExceptionEvent(Object source, @Nullable Declarable declarable, Throwable t) {
		super(source);
		this.declarable = declarable;
		this.throwable = t;
	}

	/**
	 * @return the declarable - if null, we were declaring a broker-named queue.
	 */
	public @Nullable Declarable getDeclarable() {
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
