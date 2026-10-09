/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Björn Michael
 * @since 2.4
 */
public class DeclarablesTests {

	@Test
	public void getDeclarables() {
		List<Queue> queues = List.of(
				new Queue("q1", false, false, true),
				new Queue("q2", false, false, true));
		Declarables declarables = new Declarables(queues);

		assertThat(declarables.getDeclarables()).hasSameElementsAs(queues);
	}

	@Test
	public void getDeclarablesByType() {
		Queue queue = new Queue("queue");
		TopicExchange exchange = new TopicExchange("exchange");
		Binding binding = BindingBuilder.bind(queue).to(exchange).with("foo.bar");
		Declarables declarables = new Declarables(queue, exchange, binding);

		assertThat(declarables.getDeclarablesByType(Queue.class)).containsExactlyInAnyOrder(queue);
		assertThat(declarables.getDeclarablesByType(Exchange.class)).containsExactlyInAnyOrder(exchange);
		assertThat(declarables.getDeclarablesByType(Binding.class)).containsExactlyInAnyOrder(binding);
		assertThat(declarables.getDeclarablesByType(Declarable.class)).containsExactlyInAnyOrder(
				queue, exchange, binding);
	}

}
