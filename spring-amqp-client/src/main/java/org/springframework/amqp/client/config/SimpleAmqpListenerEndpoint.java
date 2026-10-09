/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client.config;

import org.springframework.amqp.core.MessageListener;

/**
 * The {@link AbstractAmqpListenerEndpoint} implementation for {@link MessageListener}.
 * Usually makes sense for programmatic {@link org.springframework.amqp.client.listener.AmqpMessageListenerContainer}
 * registration.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see AmqpListenerEndpointRegistry
 * @see AmqpMessageListenerContainerFactory
 */
public class SimpleAmqpListenerEndpoint extends AbstractAmqpListenerEndpoint {

	private final MessageListener messageListener;

	public SimpleAmqpListenerEndpoint(MessageListener messageListener, String... addresses) {
		super(addresses);
		this.messageListener = messageListener;
	}

	@Override
	public MessageListener getMessageListener() {
		return this.messageListener;
	}

}
