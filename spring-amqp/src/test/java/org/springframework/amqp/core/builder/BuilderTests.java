/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.amqp.core.builder;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;

/**
 * @author Gary Russell
 * @since 1.6
 *
 */
public class BuilderTests {

	@Test
	public void testQueueBuilder() {
		Queue queue = QueueBuilder.durable("foo").autoDelete().exclusive().withArgument("foo", "bar").build();
		assertThat(queue.getName(), equalTo("foo"));
		assertTrue(queue.isAutoDelete());
		assertTrue(queue.isExclusive());
		assertTrue(queue.isDurable());
		assertThat((String) queue.getArguments().get("foo"), equalTo("bar"));

		queue = QueueBuilder.nonDurable().build();
		assertThat(queue.getName(), startsWith("spring.gen-"));
		assertFalse(queue.isAutoDelete());
		assertFalse(queue.isExclusive());
		assertFalse(queue.isDurable());

		queue = QueueBuilder.durable().build();
		assertThat(queue.getName(), startsWith("spring.gen-"));
		assertFalse(queue.isAutoDelete());
		assertFalse(queue.isExclusive());
		assertTrue(queue.isDurable());
	}

	@Test
	public void testExchangeBuilder() {
		Exchange exchange = ExchangeBuilder.directExchange("foo").autoDelete().delayed().internal()
				.withArgument("foo", "bar").build();
		assertThat(exchange, instanceOf(DirectExchange.class));
		assertTrue(exchange.isAutoDelete());
		assertTrue(exchange.isDurable());
		assertTrue(exchange.isInternal());
		assertTrue(exchange.isDelayed());
		assertThat((String) exchange.getArguments().get("foo"), equalTo("bar"));

		exchange = ExchangeBuilder.topicExchange("foo").durable(false).build();
		assertThat(exchange, instanceOf(TopicExchange.class));
		assertFalse(exchange.isAutoDelete());
		assertFalse(exchange.isDurable());
		assertFalse(exchange.isInternal());
		assertFalse(exchange.isDelayed());

		exchange = ExchangeBuilder.fanoutExchange("foo").build();
		assertThat(exchange, instanceOf(FanoutExchange.class));
		assertFalse(exchange.isAutoDelete());
		assertTrue(exchange.isDurable());
		assertFalse(exchange.isInternal());
		assertFalse(exchange.isDelayed());

		exchange = ExchangeBuilder.headersExchange("foo").build();
		assertThat(exchange, instanceOf(HeadersExchange.class));
		assertFalse(exchange.isAutoDelete());
		assertTrue(exchange.isDurable());
		assertFalse(exchange.isInternal());
		assertFalse(exchange.isDelayed());
	}

}
