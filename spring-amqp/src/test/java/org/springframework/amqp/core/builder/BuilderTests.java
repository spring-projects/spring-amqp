/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.core.builder;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.ConsistentHashExchange;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6
 */
public class BuilderTests {

	@Test
	public void testQueueBuilder() {
		var queue = QueueBuilder.durable("durable").autoDelete().exclusive().withArgument("key", "value").build();
		assertThat(queue.getName()).isEqualTo("durable");
		assertThat(queue.isAutoDelete()).isTrue();
		assertThat(queue.isExclusive()).isTrue();
		assertThat(queue.isDurable()).isTrue();
		assertThat((String) queue.getArguments().get("key")).isEqualTo("value");

		queue = QueueBuilder.nonDurable().build();
		assertThat(queue.getName()).startsWith("spring.gen-");
		assertThat(queue.isAutoDelete()).isFalse();
		assertThat(queue.isExclusive()).isFalse();
		assertThat(queue.isDurable()).isFalse();

		queue = QueueBuilder.durable().build();
		assertThat(queue.getName()).startsWith("spring.gen-");
		assertThat(queue.isAutoDelete()).isFalse();
		assertThat(queue.isExclusive()).isFalse();
		assertThat(queue.isDurable()).isTrue();
	}

	@Test
	public void testExchangeBuilder() {
		Exchange exchange = ExchangeBuilder.directExchange("direct").autoDelete().internal()
				.withArgument("key", "value").build();
		assertThat(exchange).isInstanceOf(DirectExchange.class);
		assertThat(exchange.isAutoDelete()).isTrue();
		assertThat(exchange.isDurable()).isTrue();
		assertThat(exchange.isInternal()).isTrue();
		assertThat((String) exchange.getArguments().get("key")).isEqualTo("value");

		exchange = ExchangeBuilder.topicExchange("topic").durable(false).build();
		assertThat(exchange).isInstanceOf(TopicExchange.class);
		assertThat(exchange.isAutoDelete()).isFalse();
		assertThat(exchange.isDurable()).isFalse();
		assertThat(exchange.isInternal()).isFalse();

		exchange = ExchangeBuilder.fanoutExchange("fanout").build();
		assertThat(exchange).isInstanceOf(FanoutExchange.class);
		assertThat(exchange.isAutoDelete()).isFalse();
		assertThat(exchange.isDurable()).isTrue();
		assertThat(exchange.isInternal()).isFalse();

		exchange = ExchangeBuilder.headersExchange("headers").build();
		assertThat(exchange).isInstanceOf(HeadersExchange.class);
		assertThat(exchange.isAutoDelete()).isFalse();
		assertThat(exchange.isDurable()).isTrue();
		assertThat(exchange.isInternal()).isFalse();

		exchange = ExchangeBuilder.consistentHashExchange("consistent")
				.ignoreDeclarationExceptions()
				.hashHeader("my_header")
				.build();

		assertThat(exchange).isInstanceOf(ConsistentHashExchange.class);
		assertThat((String) exchange.getArguments().get("hash-header")).isEqualTo("my_header");

		assertThatIllegalArgumentException()
				.isThrownBy(() ->
						ExchangeBuilder.consistentHashExchange("wrong_exchange")
								.hashHeader("my_header")
								.hashProperty("my_property")
								.build())
				.withMessage("The 'hash-header' and 'hash-property' are mutually exclusive.");

	}

}
