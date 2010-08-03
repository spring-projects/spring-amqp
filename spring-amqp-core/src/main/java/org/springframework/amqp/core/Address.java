/*
 * Copyright 2002-2010 the original author or authors.
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
 * Represents an address for publication of an AMQP message. The AMQP 0-8 and
 * 0-9 specifications have an unstructured string that is used as a "reply to"
 * address. There are however conventions in use and this class makes it easier
 * to follow these conventions.
 *
 * @author Mark Pollack
 */
public class Address {

	private static final Pattern pattern = Pattern.compile("^([^:]+)://([^/]*)/?(.*)$");


	private String unstructuredAddress;

	private ExchangeType exchangeType;

	private String exchangeName;

	private String routingKey;

	private boolean structured;


	/**
	 * Create an Address instance from an unstructured String.
	 * @param address an unstructured string.
	 */
	public Address(String address) {
		this.unstructuredAddress = address;
	}

	/***
	 * Create an Address given the exchange type, exchange name and routing key.  This
	 * will set the 
	 * @param exchangeType
	 * @param exchangeName
	 * @param routingKey
	 */
	public Address(ExchangeType exchangeType, String exchangeName, String routingKey) {		
		this.structured = true;
		this.exchangeType = exchangeType;
		this.exchangeName = exchangeName;
		this.routingKey = routingKey;
	}


	public static Address parse(String address) {
		if (address == null) {
			return null;
		}
		Matcher matcher = pattern.matcher(address);
		boolean matchFound = matcher.find();			
		if (matchFound) {
			//TODO regex not expecting as would like with grouping.
			String exchangeType = matcher.group(1);
			String exchangeName = matcher.group(2);
			String routingKey = matcher.group(3);
			return new Address(ExchangeType.valueOf(exchangeType), exchangeName, routingKey);
		} 
		return new Address(address);		
	}

	public ExchangeType getExchangeType() {
		return exchangeType;
	}

	public String getExchangeName() {
		return exchangeName;
	}

	public String getRoutingKey() {
		return routingKey;
	}

	public boolean isStructured() {
		return structured;
	}

	public String toString() {
		if (!this.structured) {
			return this.unstructuredAddress;
		}
		StringBuilder sb = new StringBuilder(this.exchangeType + "://" + this.exchangeName);
		if (StringUtils.hasText(this.routingKey)) {
			sb.append("/" + this.routingKey);
		}
		return sb.toString();
	}

}
