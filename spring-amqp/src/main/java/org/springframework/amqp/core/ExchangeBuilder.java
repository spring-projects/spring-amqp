/*
 * Copyright 2016-2019 the original author or authors.
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

import java.util.Arrays;
import java.util.Map;

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Builder providing a fluent API for building {@link Exchange}s.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6
 *
 */
public final class ExchangeBuilder extends AbstractBuilder {

	private final String name;

	private final String type;

	private boolean durable = true;

	private boolean autoDelete;

	private boolean internal;

	private boolean delayed;

	private boolean ignoreDeclarationExceptions;

	private boolean declare = true;

	private Object[] declaringAdmins;

	/**
	 * Construct an instance of the appropriate type.
	 * @param name the exchange name
	 * @param type the type name
	 * @since 1.6.7
	 * @see ExchangeTypes
	 */
	public ExchangeBuilder(String name, String type) {
		this.name = name;
		this.type = type;
	}

	/**
	 * Return a {@link DirectExchange} builder.
	 * @param name the name.
	 * @return the builder.
	 */
	public static ExchangeBuilder directExchange(String name) {
		return new ExchangeBuilder(name, ExchangeTypes.DIRECT);
	}

	/**
	 * Return a {@link TopicExchange} builder.
	 * @param name the name.
	 * @return the builder.
	 */
	public static ExchangeBuilder topicExchange(String name) {
		return new ExchangeBuilder(name, ExchangeTypes.TOPIC);
	}

	/**
	 * Return a {@link FanoutExchange} builder.
	 * @param name the name.
	 * @return the builder.
	 */
	public static ExchangeBuilder fanoutExchange(String name) {
		return new ExchangeBuilder(name, ExchangeTypes.FANOUT);
	}

	/**
	 * Return a {@link HeadersExchange} builder.
	 * @param name the name.
	 * @return the builder.
	 */
	public static ExchangeBuilder headersExchange(String name) {
		return new ExchangeBuilder(name, ExchangeTypes.HEADERS);
	}

	/**
	 * Set the auto delete flag.
	 * @return the builder.
	 */
	public ExchangeBuilder autoDelete() {
		this.autoDelete = true;
		return this;
	}

	/**
	 * Set the durable flag.
	 * @param isDurable the durable flag (default true).
	 * @return the builder.
	 */
	public ExchangeBuilder durable(boolean isDurable) {
		this.durable = isDurable;
		return this;
	}

	/**
	 * Add an argument.
	 * @param key the argument key.
	 * @param value the argument value.
	 * @return the builder.
	 */
	public ExchangeBuilder withArgument(String key, Object value) {
		getOrCreateArguments().put(key, value);
		return this;
	}

	/**
	 * Add the arguments.
	 * @param arguments the arguments map.
	 * @return the builder.
	 */
	public ExchangeBuilder withArguments(Map<String, Object> arguments) {
		this.getOrCreateArguments().putAll(arguments);
		return this;
	}

	public ExchangeBuilder alternate(String exchange) {
		return withArgument("alternate-exchange", exchange);
	}

	/**
	 * Set the internal flag.
	 * @return the builder.
	 */
	public ExchangeBuilder internal() {
		this.internal = true;
		return this;
	}

	/**
	 * Set the delayed flag.
	 * @return the builder.
	 */
	public ExchangeBuilder delayed() {
		this.delayed = true;
		return this;
	}

	/**
	 * Switch on ignore exceptions such as mismatched properties when declaring.
	 * @return the builder.
	 * @since 2.0
	 */
	public ExchangeBuilder ignoreDeclarationExceptions() {
		this.ignoreDeclarationExceptions = true;
		return this;
	}

	/**
	 * Switch to disable declaration of the exchange by any admin.
	 * @return the builder.
	 * @since 2.1
	 */
	public ExchangeBuilder suppressDeclaration() {
		this.declare = false;
		return this;
	}

	/**
	 * Admin instances, or admin bean names that should declare this exchange.
	 * @param admins the admins.
	 * @return the builder.
	 * @since 2.1
	 */
	public ExchangeBuilder admins(Object... admins) {
		Assert.notNull(admins, "'admins' cannot be null");
		Assert.noNullElements(admins, "'admins' can't have null elements");
		this.declaringAdmins = Arrays.copyOf(admins, admins.length);
		return this;
	}

	@SuppressWarnings("unchecked")
	public <T extends Exchange> T build() {
		AbstractExchange exchange;
		if (ExchangeTypes.DIRECT.equals(this.type)) {
			exchange = new DirectExchange(this.name, this.durable, this.autoDelete, getArguments());
		}
		else if (ExchangeTypes.TOPIC.equals(this.type)) {
			exchange = new TopicExchange(this.name, this.durable, this.autoDelete, getArguments());
		}
		else if (ExchangeTypes.FANOUT.equals(this.type)) {
			exchange = new FanoutExchange(this.name, this.durable, this.autoDelete, getArguments());
		}
		else if (ExchangeTypes.HEADERS.equals(this.type)) {
			exchange = new HeadersExchange(this.name, this.durable, this.autoDelete, getArguments());
		}
		else {
			exchange = new CustomExchange(this.name, this.type, this.durable, this.autoDelete, getArguments());
		}
		exchange.setInternal(this.internal);
		exchange.setDelayed(this.delayed);
		exchange.setIgnoreDeclarationExceptions(this.ignoreDeclarationExceptions);
		exchange.setShouldDeclare(this.declare);
		if (!ObjectUtils.isEmpty(this.declaringAdmins)) {
			exchange.setAdminsThatShouldDeclare(this.declaringAdmins);
		}
		return (T) exchange;
	}

}
