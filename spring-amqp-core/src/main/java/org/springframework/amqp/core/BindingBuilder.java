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

import java.util.HashMap;
import java.util.Map;

import org.springframework.util.Assert;

/**
 * Basic builder class to create bindings for a more fluent API style in code based configuration.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
public final class BindingBuilder  {

	public static ExchangeConfigurer from(Queue queue) {
		return new ExchangeConfigurer(queue);
	}


	public static class ExchangeConfigurer {

		private final Queue queue;

		private ExchangeConfigurer(Queue queue) {
			this.queue = queue;
		}

		public Binding to(FanoutExchange exchange) {
			return new Binding(this.queue, exchange);
		}

		public HeadersExchangeMapConfigurer to(HeadersExchange exchange) {
			return new HeadersExchangeMapConfigurer(this.queue, exchange);
		}

		public DirectExchangeRoutingKeyConfigurer to(DirectExchange exchange) {
			return new DirectExchangeRoutingKeyConfigurer(this.queue, exchange);
		}

		public TopicExchangeRoutingKeyConfigurer to(TopicExchange exchange) {
			return new TopicExchangeRoutingKeyConfigurer(this.queue, exchange);
		}
	}


	public static class HeadersExchangeMapConfigurer {

		protected final Queue queue;

		protected final HeadersExchange exchange;

		private HeadersExchangeMapConfigurer(Queue queue, HeadersExchange exchange) {
			this.queue = queue;
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
				return new Binding(queue, exchange, createMapForKeys(this.key));
			}

			public Binding matches(Object value) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put(key, value);
				return new Binding(queue, exchange, map);
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
				return new Binding(queue, exchange, this.headerMap);
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
				return new Binding(queue, exchange, this.headerMap);
			}
		}
	}


	private static abstract class AbstractRoutingKeyConfigurer<E extends Exchange> {

		protected final Queue queue;

		protected final E exchange;

		private AbstractRoutingKeyConfigurer(Queue queue, E exchange) {
			this.queue = queue;
			this.exchange = exchange;
		}
	}


	public static class TopicExchangeRoutingKeyConfigurer extends AbstractRoutingKeyConfigurer<TopicExchange> {

		private TopicExchangeRoutingKeyConfigurer(Queue queue, TopicExchange exchange) {
			super(queue, exchange);
		}

		public Binding with(String routingKey) {
			return new Binding(this.queue, this.exchange, routingKey);
		}

		public Binding with(Enum<?> routingKeyEnum) {
			return new Binding(this.queue, this.exchange, routingKeyEnum.toString());
		}
	}


	public static class DirectExchangeRoutingKeyConfigurer extends AbstractRoutingKeyConfigurer<DirectExchange> {

		private DirectExchangeRoutingKeyConfigurer(Queue queue, DirectExchange exchange) {
			super(queue, exchange);
		}

		public Binding with(String routingKey) {
			return new Binding(this.queue, this.exchange, routingKey);
		}

		public Binding with(Enum<?> routingKeyEnum) {
			return new Binding(this.queue, this.exchange, routingKeyEnum.toString());
		}

		public Binding withQueueName() {
			return new Binding(this.queue, this.exchange, this.queue.getName());
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
