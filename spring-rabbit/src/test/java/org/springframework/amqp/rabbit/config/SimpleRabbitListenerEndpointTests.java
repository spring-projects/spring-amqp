/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.mock;

/**
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class SimpleRabbitListenerEndpointTests {

	private final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();

	private final MessageListener messageListener = new MessageListenerAdapter();

	@Test
	public void createListener() {
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setMessageListener(messageListener);
		assertThat(endpoint.createMessageListener(container)).isSameAs(messageListener);
	}

	@Test
	public void queueAndQueueNamesSet() {
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setMessageListener(messageListener);

		endpoint.setQueueNames("foo", "bar");
		endpoint.setQueues(mock(Queue.class));

		assertThatIllegalStateException()
			.isThrownBy(() -> endpoint.setupListenerContainer(container));
	}

}
