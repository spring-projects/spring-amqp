/*
 * Copyright 2016-present the original author or authors.
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
 * Builder providing a fluent API for building {@link Exchange}s.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6
 */
public class ExchangeBuilder extends BaseExchangeBuilder<ExchangeBuilder> {

	/**
	 * Construct an instance of the appropriate type.
	 * @param name the exchange name
	 * @param type the type name
	 * @since 1.6.7
	 * @see ExchangeTypes
	 */
	public ExchangeBuilder(String name, String type) {
		super(name, type);
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
	 * Return an {@code x-consistent-hash} exchange builder.
	 * @param name the name.
	 * @return the builder.
	 * @since 3.2
	 */
	public static ConsistentHashExchangeBuilder consistentHashExchange(String name) {
		return new ConsistentHashExchangeBuilder(name);
	}

	/**
	 * An {@link ExchangeBuilder} extension for the {@link ConsistentHashExchange}.
	 *
	 * @since 3.2
	 */
	public static final class ConsistentHashExchangeBuilder extends BaseExchangeBuilder<ConsistentHashExchangeBuilder> {

		/**
		 * Construct an instance of the builder for {@link ConsistentHashExchange}.
		 *
		 * @param name the exchange name
		 * @see ExchangeTypes
		 */
		public ConsistentHashExchangeBuilder(String name) {
			super(name, ExchangeTypes.CONSISTENT_HASH);
		}

		public ConsistentHashExchangeBuilder hashHeader(String headerName) {
			withArgument("hash-header", headerName);
			return this;
		}

		public ConsistentHashExchangeBuilder hashProperty(String propertyName) {
			withArgument("hash-property", propertyName);
			return this;
		}

		@Override
		@SuppressWarnings("unchecked")
		public ConsistentHashExchange build() {
			return configureExchange(
					new ConsistentHashExchange(this.name, this.durable, this.autoDelete, getArguments()));
		}

	}

}
