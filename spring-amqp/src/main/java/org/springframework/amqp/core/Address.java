/*
 * Copyright 2002-2016 the original author or authors.
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * @author Gary Russell
 */
public class Address {

	/**
	 * Use this value in {@code RabbitTemplate#setReplyAddress(String)} to explicitly
	 * indicate that direct reply-to is to be used.
	 */
	public static final String AMQ_RABBITMQ_REPLY_TO = "amq.rabbitmq.reply-to";

	private static final Pattern pattern = Pattern.compile("^(?:.*://)?([^/]*)/?(.*)$");

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
		else if (address.startsWith(AMQ_RABBITMQ_REPLY_TO)) {
			this.routingKey = address;
			this.exchangeName = "";
		}
		else if (address.lastIndexOf('/') <= 0) {
			this.routingKey = address.replaceFirst("/", "");
			this.exchangeName = "";
		}
		else {
			Matcher matcher = pattern.matcher(address);
			boolean matchFound = matcher.find();
			if (matchFound) {
				this.exchangeName = matcher.group(1);
				this.routingKey = matcher.group(2);
			}
			else {
				this.exchangeName = "";
				this.routingKey = address;
			}
		}
	}

	/***
	 * Create an Address given the exchange type, exchange name and routing key.
	 * This will set the exchange type, name and the routing key explicitly.
	 * @param exchangeType The exchange type.
	 * @param exchangeName The exchange name.
	 * @param routingKey The routing key.
	 */
	@Deprecated
	public Address(String exchangeType, String exchangeName, String routingKey) {
		this.exchangeName = exchangeName;
		this.routingKey = routingKey;
	}

	/***
	 * Create an Address given the exchange name and routing key.
	 * This will set the exchange type, name and the routing key explicitly.
	 * @param exchangeName The exchange name.
	 * @param routingKey The routing key.
	 */
	public Address(String exchangeName, String routingKey) {
		this.exchangeName = exchangeName;
		this.routingKey = routingKey;
	}

	@Deprecated
	public String getExchangeType() {
		return null;
	}

	public String getExchangeName() {
		return this.exchangeName;
	}

	public String getRoutingKey() {
		return this.routingKey;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Address address = (Address) o;

		return !(this.exchangeName != null
				? !this.exchangeName.equals(address.exchangeName)
				: address.exchangeName != null)
				&& !(this.routingKey != null
				? !this.routingKey.equals(address.routingKey)
				: address.routingKey != null);
	}

	@Override
	public int hashCode() {
		int result = this.exchangeName != null ? this.exchangeName.hashCode() : 0;
		result = 31 * result + (this.routingKey != null ? this.routingKey.hashCode() : 0);
		return result;
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
