/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.rabbit.stream.micrometer;

import java.util.Map;

import com.rabbitmq.stream.Message;
import io.micrometer.observation.transport.SenderContext;

/**
 * {@link SenderContext} for {@link Message}s.
 *
 * @author Gary Russell
 * @since 3.0.5
 *
 */
public class RabbitStreamMessageSenderContext extends SenderContext<Message> {

	private final String beanName;

	private final String destination;

	@SuppressWarnings("this-escape")
	public RabbitStreamMessageSenderContext(Message message, String beanName, String destination) {
		super((carrier, key, value) -> {
			Map<String, Object> props = message.getApplicationProperties();
			if (props != null) {
				props.put(key, value);
			}
		});
		setCarrier(message);
		this.beanName = beanName;
		this.destination = destination;
		setRemoteServiceName("RabbitMQ Stream");
	}

	public String getBeanName() {
		return this.beanName;
	}

	/**
	 * Return the destination - {@code exchange/routingKey}.
	 * @return the destination.
	 */
	public String getDestination() {
		return this.destination;
	}

}
