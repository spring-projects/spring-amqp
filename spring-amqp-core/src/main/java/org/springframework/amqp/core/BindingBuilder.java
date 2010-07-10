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

		public DirectExchangeRoutingKeyConfigurer to(DirectExchange exchange) {
			return new DirectExchangeRoutingKeyConfigurer(this.queue, exchange);
		}

		public RoutingKeyConfigurer to(Exchange exchange) {
			return new RoutingKeyConfigurer(this.queue, exchange);
		}
	}


	public static class RoutingKeyConfigurer {

		protected final Queue queue;

		protected final Exchange exchange;

		private RoutingKeyConfigurer(Queue queue, Exchange exchange) {
			this.queue = queue;
			this.exchange = exchange;
		}

		public Binding with(String routingKey) {
			return new Binding(this.queue, this.exchange, routingKey);
		}

		@SuppressWarnings("unchecked")
		public Binding with(Enum routingKeyEnum) {
			return new Binding(this.queue, this.exchange, routingKeyEnum.toString());
		}
	}


	public static class DirectExchangeRoutingKeyConfigurer extends RoutingKeyConfigurer {

		private DirectExchangeRoutingKeyConfigurer(Queue queue, Exchange exchange) {
			super(queue, exchange);
		}

		public Binding withQueueName() {
			return new Binding(this.queue, this.exchange, this.queue.getName());
		}
	}

}
