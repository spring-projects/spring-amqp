/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.Map;

/**
 * Interface for all exchanges.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Artem Bilan
 */
public interface Exchange extends Declarable {

	/**
	 * The name of the exchange.
	 *
	 * @return the name of the exchange.
	 */
	String getName();

	/**
	 * The type of the exchange. See {@link ExchangeTypes} for some well-known examples.
	 *
	 * @return the type of the exchange.
	 */
	String getType();

	/**
	 * A durable exchange will survive a server restart.
	 *
	 * @return true if durable.
	 */
	boolean isDurable();

	/**
	 * True if the server should delete the exchange when it is no longer in use (if all bindings are deleted).
	 *
	 * @return true if auto-delete.
	 */
	boolean isAutoDelete();

	/**
	 * A map of arguments used to declare the exchange. These are stored by the broker, but do not necessarily have any
	 * meaning to the broker (depending on the exchange type).
	 *
	 * @return the arguments.
	 */
	Map<String, Object> getArguments();

	/**
	 * Is a delayed message exchange; currently requires a broker plugin.
	 * @return true if delayed.
	 * @since 1.6
	 */
	boolean isDelayed();

	/**
	 * Is an exchange internal; i.e. can't be directly published to by a client,
	 * used for exchange-to-exchange binding only.
	 * @return true if internal.
	 * @since 1.6
	 */
	boolean isInternal();

}
