/*
 * Copyright 2002-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.core;

/**
 * Constants for the standard Exchange type names.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Artem Bilan
 */
public final class ExchangeTypes {

	/**
	 * Direct exchange.
	 */
	public static final String DIRECT = "direct";

	/**
	 * Topic exchange.
	 */
	public static final String TOPIC = "topic";

	/**
	 * Fanout exchange.
	 */
	public static final String FANOUT = "fanout";

	/**
	 * Headers exchange.
	 */
	public static final String HEADERS = "headers";

	/**
	 * Consistent Hash exchange.
	 * @since 3.2
	 */
	public static final String CONSISTENT_HASH = "x-consistent-hash";

	/**
	 * System exchange.
	 * @deprecated with no replacement (for removal): there is no such an exchange type in AMQP.
	 */
	@Deprecated(since = "3.2", forRemoval = true)
	public static final String SYSTEM = "system";

	private ExchangeTypes() {
	}

}
