/*
 * Copyright 2002-2025 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.util.Assert;

/**
 * Basic builder class to create bindings for a more fluent API style in code based configuration.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Artem Bilan
 */
public final class BindingBuilder {

	private BindingBuilder() {
	}

	public static DestinationConfigurer bind(Queue queue) {
		if (queue.getName().isEmpty()) {
			return new DestinationConfigurer(queue, DestinationType.QUEUE);
		}
		else {
			return new DestinationConfigurer(queue.getName(), DestinationType.QUEUE);
		}
	}

	public static DestinationConfigurer bind(Exchange exchange) {
		return new DestinationConfigurer(exchange.getName(), DestinationType.EXCHANGE);
	}

	private static Map<String, @Nullable Object> createMapForKeys(String... keys) {
		Map<String, @Nullable Object> map = new HashMap<>();
		for (String key : keys) {
			map.put(key, null);
		}
		return map;
	}

	/**
	 * General destination configurer.
	 */
	public static final class DestinationConfigurer {

		protected final @Nullable String name; // NOSONAR

		protected final DestinationType type; // NOSONAR

		protected final @Nullable Queue queue; // NOSONAR

		DestinationConfigurer(String name, DestinationType type) {
			this.queue = null;
			this.name = name;
			this.type = type;
		}

		DestinationConfigurer(Queue queue, DestinationType type) {
			this.queue = queue;
			this.name = null;
			this.type = type;
		}

		public Binding to(FanoutExchange exchange) {
			return new Binding(this.queue, this.name, this.type, exchange.getName(), "", new HashMap<>());
		}

		public HeadersExchangeMapConfigurer to(HeadersExchange exchange) {
			return new HeadersExchangeMapConfigurer(this, exchange);
		}

		public DirectExchangeRoutingKeyConfigurer to(DirectExchange exchange) {
			return new DirectExchangeRoutingKeyConfigurer(this, exchange);
		}

		public TopicExchangeRoutingKeyConfigurer to(TopicExchange exchange) {
			return new TopicExchangeRoutingKeyConfigurer(this, exchange);
		}

		public GenericExchangeRoutingKeyConfigurer to(Exchange exchange) {
			return new GenericExchangeRoutingKeyConfigurer(this, exchange);
		}

	}

	/**
	 * Headers exchange configurer.
	 */
	public static final class HeadersExchangeMapConfigurer {

		protected final DestinationConfigurer destination; // NOSONAR

		protected final HeadersExchange exchange; // NOSONAR

		HeadersExchangeMapConfigurer(DestinationConfigurer destination, HeadersExchange exchange) {
			this.destination = destination;
			this.exchange = exchange;
		}

		public HeadersExchangeSingleValueBindingCreator where(String key) {
			return new HeadersExchangeSingleValueBindingCreator(key);
		}

		public HeadersExchangeKeysBindingCreator whereAny(String... headerKeys) {
			return new HeadersExchangeKeysBindingCreator(headerKeys, false);
		}

		public HeadersExchangeMapBindingCreator whereAny(Map<String, Object> headerValues) {
			return new HeadersExchangeMapBindingCreator(headerValues, false);
		}

		public HeadersExchangeKeysBindingCreator whereAll(String... headerKeys) {
			return new HeadersExchangeKeysBindingCreator(headerKeys, true);
		}

		public HeadersExchangeMapBindingCreator whereAll(Map<String, Object> headerValues) {
			return new HeadersExchangeMapBindingCreator(headerValues, true);
		}

		/**
		 * Headers exchange single value binding creator.
		 */
		public final class HeadersExchangeSingleValueBindingCreator {

			private final String key;

			HeadersExchangeSingleValueBindingCreator(String key) {
				Assert.notNull(key, "key must not be null");
				this.key = key;
			}

			public Binding exists() {
				return new Binding(HeadersExchangeMapConfigurer.this.destination.queue,
						HeadersExchangeMapConfigurer.this.destination.name,
						HeadersExchangeMapConfigurer.this.destination.type,
						HeadersExchangeMapConfigurer.this.exchange.getName(), "", createMapForKeys(this.key));
			}

			public Binding matches(Object value) {
				Map<String, @Nullable Object> map = new HashMap<>();
				map.put(this.key, value);
				return new Binding(HeadersExchangeMapConfigurer.this.destination.queue,
						HeadersExchangeMapConfigurer.this.destination.name,
						HeadersExchangeMapConfigurer.this.destination.type,
						HeadersExchangeMapConfigurer.this.exchange.getName(), "", map);
			}

		}

		/**
		 * Headers exchange keys binding creator.
		 */
		public final class HeadersExchangeKeysBindingCreator {

			private final Map<String, @Nullable Object> headerMap;

			HeadersExchangeKeysBindingCreator(String[] headerKeys, boolean matchAll) {
				Assert.notEmpty(headerKeys, "header key list must not be empty");
				this.headerMap = createMapForKeys(headerKeys);
				this.headerMap.put("x-match", (matchAll ? "all" : "any"));
			}

			public Binding exist() {
				return new Binding(HeadersExchangeMapConfigurer.this.destination.queue,
						HeadersExchangeMapConfigurer.this.destination.name,
						HeadersExchangeMapConfigurer.this.destination.type,
						HeadersExchangeMapConfigurer.this.exchange.getName(), "", this.headerMap);
			}

		}

		/**
		 * Headers exchange map binding creator.
		 */
		public final class HeadersExchangeMapBindingCreator {

			private final Map<String, @Nullable Object> headerMap;

			HeadersExchangeMapBindingCreator(Map<String, Object> headerMap, boolean matchAll) {
				Assert.notEmpty(headerMap, "header map must not be empty");
				this.headerMap = new HashMap<>(headerMap);
				this.headerMap.put("x-match", (matchAll ? "all" : "any"));
			}

			public Binding match() {
				return new Binding(HeadersExchangeMapConfigurer.this.destination.queue,
						HeadersExchangeMapConfigurer.this.destination.name,
						HeadersExchangeMapConfigurer.this.destination.type,
						HeadersExchangeMapConfigurer.this.exchange.getName(), "", this.headerMap);
			}

		}

	}

	private abstract static class AbstractRoutingKeyConfigurer {

		protected final DestinationConfigurer destination; // NOSONAR

		protected final String exchange; // NOSONAR

		AbstractRoutingKeyConfigurer(DestinationConfigurer destination, String exchange) {
			this.destination = destination;
			this.exchange = exchange;
		}

	}

	/**
	 * Topic exchange routing key configurer.
	 */
	public static final class TopicExchangeRoutingKeyConfigurer extends AbstractRoutingKeyConfigurer {

		TopicExchangeRoutingKeyConfigurer(DestinationConfigurer destination, TopicExchange exchange) {
			super(destination, exchange.getName());
		}

		public Binding with(String routingKey) {
			return new Binding(destination.queue, destination.name, destination.type, exchange, routingKey, null);
		}

		public Binding with(Enum<?> routingKeyEnum) {
			return new Binding(destination.queue, destination.name, destination.type, exchange,
					routingKeyEnum.toString(), null);
		}

	}

	/**
	 * Generic exchange routing key configurer.
	 */
	public static final class GenericExchangeRoutingKeyConfigurer extends AbstractRoutingKeyConfigurer {

		GenericExchangeRoutingKeyConfigurer(DestinationConfigurer destination, Exchange exchange) {
			super(destination, exchange.getName());
		}

		public GenericArgumentsConfigurer with(String routingKey) {
			return new GenericArgumentsConfigurer(this, routingKey);
		}

		public GenericArgumentsConfigurer with(Enum<?> routingKeyEnum) {
			return new GenericArgumentsConfigurer(this, routingKeyEnum.toString());
		}

	}

	/**
	 * Generic argument configurer.
	 */
	public static class GenericArgumentsConfigurer {

		private final GenericExchangeRoutingKeyConfigurer configurer;

		private final String routingKey;

		public GenericArgumentsConfigurer(GenericExchangeRoutingKeyConfigurer configurer, String routingKey) {
			this.configurer = configurer;
			this.routingKey = routingKey;
		}

		public Binding and(Map<String, @Nullable Object> map) {
			return new Binding(this.configurer.destination.queue,
					this.configurer.destination.name, this.configurer.destination.type, this.configurer.exchange,
					this.routingKey, map);
		}

		public Binding noargs() {
			return new Binding(this.configurer.destination.queue,
					this.configurer.destination.name, this.configurer.destination.type, this.configurer.exchange,
					this.routingKey, null);
		}

	}

	/**
	 * Direct exchange routing key configurer.
	 */
	public static final class DirectExchangeRoutingKeyConfigurer extends AbstractRoutingKeyConfigurer {

		DirectExchangeRoutingKeyConfigurer(DestinationConfigurer destination, DirectExchange exchange) {
			super(destination, exchange.getName());
		}

		public Binding with(String routingKey) {
			return new Binding(destination.queue, destination.name, destination.type, exchange, routingKey, null);
		}

		public Binding with(Enum<?> routingKeyEnum) {
			return new Binding(destination.queue, destination.name, destination.type, exchange,
					routingKeyEnum.toString(), null);
		}

		public Binding withQueueName() {
			return new Binding(destination.queue, destination.name, destination.type, exchange, destination.name, null);
		}

	}

}
