/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.core;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

/**
 * Tests for the default methods of {@link AmqpAdmin} that declare several objects.
 *
 * @author Sharang Gupta
 *
 * @since 4.2
 */
class AmqpAdminDefaultMethodsTests {

	@Test
	void batchDeclarationsDeclareOneByOneByDefault() {
		AmqpAdmin admin = mock(AmqpAdmin.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		DirectExchange exchange1 = new DirectExchange("e1");
		DirectExchange exchange2 = new DirectExchange("e2");
		Queue queue1 = new Queue("q1");
		Queue queue2 = new Queue("q2");
		Binding binding1 = BindingBuilder.bind(queue1).to(exchange1).with("k1");
		Binding binding2 = BindingBuilder.bind(queue2).to(exchange2).with("k2");

		admin.declareExchanges(exchange1, exchange2);
		admin.declareQueues(queue1, queue2);
		admin.declareBindings(binding1, binding2);

		var inOrder = inOrder(admin);
		inOrder.verify(admin).declareExchange(exchange1);
		inOrder.verify(admin).declareExchange(exchange2);
		inOrder.verify(admin).declareQueue(queue1);
		inOrder.verify(admin).declareQueue(queue2);
		inOrder.verify(admin).declareBinding(binding1);
		inOrder.verify(admin).declareBinding(binding2);
	}

}
