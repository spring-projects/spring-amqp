/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Simple container collecting information to describe a direct exchange.
 * Used in conjunction with administrative operations.
 *
 * @author Mark Pollack
 * @author Dave Syer
 * @author Artem Bilan
 *
 * @see AmqpAdmin
 */
public class DirectExchange extends AbstractExchange {

	/**
	 * The default exchange.
	 */
	public static final DirectExchange DEFAULT = new DirectExchange("");


	public DirectExchange(String name) {
		super(name);
	}

	public DirectExchange(String name, boolean durable, boolean autoDelete) {
		super(name, durable, autoDelete);
	}

	public DirectExchange(String name, boolean durable, boolean autoDelete,
			@Nullable Map<String, @Nullable Object> arguments) {

		super(name, durable, autoDelete, arguments);
	}

	@Override
	public final String getType() {
		return ExchangeTypes.DIRECT;
	}

}
