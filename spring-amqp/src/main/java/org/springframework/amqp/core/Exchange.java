/*
 * Copyright 2002-2017 the original author or authors.
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
