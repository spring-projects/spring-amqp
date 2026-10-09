/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.listener;

import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateRequeueAmqpException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 4.1
 *
 */
public class ContainerUtilsTests {

	@Test
	void testMustRequeue() {
		assertThat(ContainerUtils.shouldRequeue(false,
				new ListenerExecutionFailedException("", new ImmediateRequeueAmqpException("requeue")),
				mock(Log.class)))
				.isTrue();
	}

	@Test
	void testMustNotRequeue() {
		assertThat(ContainerUtils.shouldRequeue(true,
				new ListenerExecutionFailedException("", new AmqpRejectAndDontRequeueException("no requeue")),
				mock(Log.class)))
				.isFalse();
	}

}
