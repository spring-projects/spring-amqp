/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.core;

import org.springframework.util.StringUtils;

/**
 * Represents an address for publication of an AMQP message. The AMQP 0-8 and 0-9
 * specifications have an unstructured string that is used as a "reply to" address.
 * There are however conventions in use and this class makes it easier to
 * follow these conventions, which can be easily summarised as:
 *
 * <pre class="code">
 * (exchange)/(routingKey)
 * </pre>
 *
 * Here we also the exchange name to default to empty
 * (so just a routing key will work if you know the queue name).
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Artem Bilan
 */
public class Address {

	private final String exchangeName;

	private final String routingKey;

	/**
	 * Create an Address instance from a structured String in the form
	 *
	 * <pre class="code">
	 * (exchange)/(routingKey)
	 * </pre>
	 *
	 * @param address a structured string.
	 */
	public Address(String address) {
		if (!StringUtils.hasText(address)) {
			this.exchangeName = "";
			this.routingKey = "";
		}
		else {
			String[] values = new String[] {"", ""};
			if (address.contains("/")) {
				String[] groups = address.split("/");
				System.arraycopy(groups, 0, values, 0, groups.length);
			}
			else {
				values[1] = address;
			}
			this.exchangeName = values[0];
			this.routingKey = values[1];
		}
	}

	/***
	 * Create an Address given the exchange type, exchange name and routing key.
	 * This will set the exchange type, name and the routing key explicitly.
	 * @param exchangeName The exchange name.
	 * @param routingKey The routing key.
	 */
	public Address(String exchangeName, String routingKey) {
		this.exchangeName = exchangeName;
		this.routingKey = routingKey;
	}

	public String getExchangeName() {
		return this.exchangeName;
	}

	public String getRoutingKey() {
		return this.routingKey;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(this.exchangeName + "/");
		if (StringUtils.hasText(this.routingKey)) {
			sb.append(this.routingKey);
		}
		return sb.toString();
	}

}
