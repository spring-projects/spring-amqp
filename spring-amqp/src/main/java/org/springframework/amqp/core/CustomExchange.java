/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Simple container collecting information to describe a custom exchange. Custom exchange types are allowed by the AMQP
 * specification, and their names should start with "x-" (but this is not enforced here). Used in conjunction with
 * administrative operations.
 *
 * @author Dave Syer
 * @author Artem Bilan
 *
 * @see AmqpAdmin
 */
public class CustomExchange extends AbstractExchange {

	private final String type;

	public CustomExchange(String name, String type) {
		super(name);
		this.type = type;
	}

	public CustomExchange(String name, String type, boolean durable, boolean autoDelete) {
		super(name, durable, autoDelete);
		this.type = type;
	}

	public CustomExchange(String name, String type, boolean durable, boolean autoDelete,
			@Nullable Map<String, @Nullable Object> arguments) {

		super(name, durable, autoDelete, arguments);
		this.type = type;
	}

	@Override
	public final String getType() {
		return this.type;
	}

}
