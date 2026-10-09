/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Simple container collecting information to describe a topic exchange.
 * Used in conjunction with administrative operations.
 *
 * @author Mark Pollack
 * @author Dave Syer
 * @author Artem Bilan
 *
 * @see AmqpAdmin
 */
public class TopicExchange extends AbstractExchange {

	public TopicExchange(String name) {
		super(name);
	}

	public TopicExchange(String name, boolean durable, boolean autoDelete) {
		super(name, durable, autoDelete);
	}

	public TopicExchange(String name, boolean durable, boolean autoDelete,
			@Nullable Map<String, @Nullable Object> arguments) {

		super(name, durable, autoDelete, arguments);
	}

	@Override
	public final String getType() {
		return ExchangeTypes.TOPIC;
	}

}
