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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.util.Assert;

/**
 * Basic builder class to create bindings for a more fluent API style in code based configuration.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 */
public final class BindingBuilder {

	public static DestinationConfigurer bind(Queue queue) {
		return new DestinationConfigurer(queue.getName(), DestinationType.QUEUE);
	}

	public static DestinationConfigurer bind(Exchange exchange) {
		return new DestinationConfigurer(exchange.getName(), DestinationType.EXCHANGE);
	}

	public static class DestinationConfigurer {

		protected final String name;
		protected final DestinationType type;

		private DestinationConfigurer(String name, DestinationType type) {
			this.name = name;
			this.type = type;
		}

		public Binding to(FanoutExchange exchange) {
			return new Binding(this.name, this.type, exchange.getName(), "", new HashMap<String, Object>());
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

	public static class HeadersExchangeMapConfigurer {

		protected final DestinationConfigurer destination;

		protected final HeadersExchange exchange;

		private HeadersExchangeMapConfigurer(DestinationConfigurer destination, HeadersExchange exchange) {
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

		public class HeadersExchangeSingleValueBindingCreator {

			private final String key;

			private HeadersExchangeSingleValueBindingCreator(String key) {
				Assert.notNull(key, "key must not be null");
				this.key = key;
			}

			public Binding exists() {
				return new Binding(HeadersExchangeMapConfigurer.this.destination.name,
						HeadersExchangeMapConfigurer.this.destination.type,
						HeadersExchangeMapConfigurer.this.exchange.getName(), "", createMapForKeys(this.key));
			}

			public Binding matches(Object value) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put(this.key, value);
				return new Binding(HeadersExchangeMapConfigurer.this.destination.name,
						HeadersExchangeMapConfigurer.this.destination.type,
						HeadersExchangeMapConfigurer.this.exchange.getName(), "", map);
			}
		}

		public class HeadersExchangeKeysBindingCreator {

			private final Map<String, Object> headerMap;

			private HeadersExchangeKeysBindingCreator(String[] headerKeys, boolean matchAll) {
				Assert.notEmpty(headerKeys, "header key list must not be empty");
				this.headerMap = createMapForKeys(headerKeys);
				this.headerMap.put("x-match", (matchAll ? "all" : "any"));
			}

			public Binding exist() {
				return new Binding(HeadersExchangeMapConfigurer.this.destination.name,
						HeadersExchangeMapConfigurer.this.destination.type,
						HeadersExchangeMapConfigurer.this.exchange.getName(), "", this.headerMap);
			}
		}

		public class HeadersExchangeMapBindingCreator {

			private final Map<String, Object> headerMap;

			private HeadersExchangeMapBindingCreator(Map<String, Object> headerMap, boolean matchAll) {
				Assert.notEmpty(headerMap, "header map must not be empty");
				this.headerMap = new HashMap<String, Object>(headerMap);
				this.headerMap.put("x-match", (matchAll ? "all" : "any"));
			}

			public Binding match() {
				return new Binding(HeadersExchangeMapConfigurer.this.destination.name,
						HeadersExchangeMapConfigurer.this.destination.type,
						HeadersExchangeMapConfigurer.this.exchange.getName(), "", this.headerMap);
			}
		}
	}

	private abstract static class AbstractRoutingKeyConfigurer<E extends Exchange> {

		protected final DestinationConfigurer destination;

		protected final String exchange;

		private AbstractRoutingKeyConfigurer(DestinationConfigurer destination, String exchange) {
			this.destination = destination;
			this.exchange = exchange;
		}
	}

	public static class TopicExchangeRoutingKeyConfigurer extends AbstractRoutingKeyConfigurer<TopicExchange> {

		private TopicExchangeRoutingKeyConfigurer(DestinationConfigurer destination, TopicExchange exchange) {
			super(destination, exchange.getName());
		}

		public Binding with(String routingKey) {
			return new Binding(destination.name, destination.type, exchange, routingKey,
					Collections.<String, Object> emptyMap());
		}

		public Binding with(Enum<?> routingKeyEnum) {
			return new Binding(destination.name, destination.type, exchange, routingKeyEnum.toString(),
					Collections.<String, Object> emptyMap());
		}
	}

	public static class GenericExchangeRoutingKeyConfigurer extends AbstractRoutingKeyConfigurer<TopicExchange> {

		private GenericExchangeRoutingKeyConfigurer(DestinationConfigurer destination, Exchange exchange) {
			super(destination, exchange.getName());
		}

		public GenericArgumentsConfigurer with(String routingKey) {
			return new GenericArgumentsConfigurer(this, routingKey);
		}

		public GenericArgumentsConfigurer with(Enum<?> routingKeyEnum) {
			return new GenericArgumentsConfigurer(this, routingKeyEnum.toString());
		}

	}

	public static class GenericArgumentsConfigurer {

		private final GenericExchangeRoutingKeyConfigurer configurer;
		private final String routingKey;

		public GenericArgumentsConfigurer(GenericExchangeRoutingKeyConfigurer configurer, String routingKey) {
			this.configurer = configurer;
			this.routingKey = routingKey;
		}

		public Binding and(Map<String, Object> map) {
			return new Binding(this.configurer.destination.name, this.configurer.destination.type, this.configurer.exchange,
					this.routingKey, map);
		}

		public Binding noargs() {
			return new Binding(this.configurer.destination.name, this.configurer.destination.type, this.configurer.exchange,
					this.routingKey, Collections.<String, Object> emptyMap());
		}

	}

	public static class DirectExchangeRoutingKeyConfigurer extends AbstractRoutingKeyConfigurer<DirectExchange> {

		private DirectExchangeRoutingKeyConfigurer(DestinationConfigurer destination, DirectExchange exchange) {
			super(destination, exchange.getName());
		}

		public Binding with(String routingKey) {
			return new Binding(destination.name, destination.type, exchange, routingKey,
					Collections.<String, Object> emptyMap());
		}

		public Binding with(Enum<?> routingKeyEnum) {
			return new Binding(destination.name, destination.type, exchange, routingKeyEnum.toString(),
					Collections.<String, Object> emptyMap());
		}

		public Binding withQueueName() {
			return new Binding(destination.name, destination.type, exchange, destination.name,
					Collections.<String, Object> emptyMap());
		}
	}

	private static Map<String, Object> createMapForKeys(String... keys) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (String key : keys) {
			map.put(key, null);
		}
		return map;
	}

}
