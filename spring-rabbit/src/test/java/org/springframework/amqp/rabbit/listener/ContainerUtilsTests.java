/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.rabbit.listener.support.ContainerUtils;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 2.1.8
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
