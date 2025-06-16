/*
 * Copyright 2024-present the original author or authors.
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
 * An {@link AbstractBuilder} extension for generics support.
 *
 * @param <B> the target class implementation type.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 3.2
 *
 */
public abstract class BaseExchangeBuilder<B extends BaseExchangeBuilder<B>> extends AbstractBuilder {

	protected final String name;

	protected final String type;

	protected boolean durable = true;

	protected boolean autoDelete;

	protected boolean internal;

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
	public BaseExchangeBuilder(String name, String type) {
		this.name = name;
		this.type = type;
	}


	/**
	 * Set the auto delete flag.
	 * @return the builder.
	 */
	public B autoDelete() {
		this.autoDelete = true;
		return _this();
	}

	/**
	 * Set the durable flag.
	 * @param isDurable the durable flag (default true).
	 * @return the builder.
	 */
	public B durable(boolean isDurable) {
		this.durable = isDurable;
		return _this();
	}

	/**
	 * Add an argument.
	 * @param key the argument key.
	 * @param value the argument value.
	 * @return the builder.
	 */
	public B withArgument(String key, Object value) {
		getOrCreateArguments().put(key, value);
		return _this();
	}

	/**
	 * Add the arguments.
	 * @param arguments the arguments map.
	 * @return the builder.
	 */
	public B withArguments(Map<String, Object> arguments) {
		this.getOrCreateArguments().putAll(arguments);
		return _this();
	}

	public B alternate(String exchange) {
		return withArgument("alternate-exchange", exchange);
	}

	/**
	 * Set the internal flag.
	 * @return the builder.
	 */
	public B internal() {
		this.internal = true;
		return _this();
	}

	/**
	 * Set the delayed flag.
	 * @return the builder.
	 */
	public B delayed() {
		this.delayed = true;
		return _this();
	}

	/**
	 * Switch on ignore exceptions such as mismatched properties when declaring.
	 * @return the builder.
	 * @since 2.0
	 */
	public B ignoreDeclarationExceptions() {
		this.ignoreDeclarationExceptions = true;
		return _this();
	}

	/**
	 * Switch to disable declaration of the exchange by any admin.
	 * @return the builder.
	 * @since 2.1
	 */
	public B suppressDeclaration() {
		this.declare = false;
		return _this();
	}

	/**
	 * Admin instances, or admin bean names that should declare this exchange.
	 * @param admins the admins.
	 * @return the builder.
	 * @since 2.1
	 */
	public B admins(Object... admins) {
		Assert.notNull(admins, "'admins' cannot be null");
		Assert.noNullElements(admins, "'admins' can't have null elements");
		this.declaringAdmins = Arrays.copyOf(admins, admins.length);
		return _this();
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

		return (T) configureExchange(exchange);
	}


	protected <T extends AbstractExchange> T configureExchange(T exchange) {
		exchange.setInternal(this.internal);
		exchange.setDelayed(this.delayed);
		exchange.setIgnoreDeclarationExceptions(this.ignoreDeclarationExceptions);
		exchange.setShouldDeclare(this.declare);
		if (!ObjectUtils.isEmpty(this.declaringAdmins)) {
			exchange.setAdminsThatShouldDeclare(this.declaringAdmins);
		}
		return exchange;
	}

	@SuppressWarnings("unchecked")
	protected final B _this() {
		return (B) this;
	}

}
