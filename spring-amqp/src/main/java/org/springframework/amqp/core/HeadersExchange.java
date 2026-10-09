/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Headers exchange.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Artem Bilan
 */
public class HeadersExchange extends AbstractExchange {

	public HeadersExchange(String name) {
		super(name);
	}

	public HeadersExchange(String name, boolean durable, boolean autoDelete) {
		super(name, durable, autoDelete);
	}

	public HeadersExchange(String name, boolean durable, boolean autoDelete,
			@Nullable Map<String, @Nullable Object> arguments) {

		super(name, durable, autoDelete, arguments);
	}

	@Override
	public String getType() {
		return ExchangeTypes.HEADERS;
	}

}
