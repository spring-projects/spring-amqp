/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.amqp.rabbit.support.micrometer;

import io.micrometer.observation.transport.ReceiverContext;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;

/**
 * {@link ReceiverContext} for {@link Message}s.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 3.0
 *
 */
public class RabbitMessageReceiverContext extends ReceiverContext<Message> {

	private final String listenerId;

	private final Message message;

	@SuppressWarnings("this-escape")
	public RabbitMessageReceiverContext(Message message, String listenerId) {
		super((carrier, key) -> carrier.getMessageProperties().getHeader(key));
		setCarrier(message);
		this.message = message;
		this.listenerId = listenerId;
		setRemoteServiceName("RabbitMQ");
	}

	@Override
	public Message getCarrier() {
		return this.message;
	}

	public String getListenerId() {
		return this.listenerId;
	}

	/**
	 * Return the source (queue) for this message.
	 * @return the source.
	 */
	public @Nullable String getSource() {
		return this.message.getMessageProperties().getConsumerQueue();
	}

}
