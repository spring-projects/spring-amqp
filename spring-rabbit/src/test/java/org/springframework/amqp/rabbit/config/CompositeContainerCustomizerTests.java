/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.listener.MessageListenerContainer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 2.4.8
 *
 */
public class CompositeContainerCustomizerTests {

	@SuppressWarnings("unchecked")
	@Test
	void allCalled() {
		ContainerCustomizer<MessageListenerContainer> mock1 = mock(ContainerCustomizer.class);
		ContainerCustomizer<MessageListenerContainer> mock2 = mock(ContainerCustomizer.class);
		CompositeContainerCustomizer<MessageListenerContainer> cust = new CompositeContainerCustomizer<>(
				List.of(mock1, mock2));
		MessageListenerContainer mlc = mock(MessageListenerContainer.class);
		cust.configure(mlc);
		verify(mock1).configure(mlc);
		verify(mock2).configure(mlc);
	}

}
