/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import java.io.Serial;

import org.springframework.amqp.event.AmqpEvent;

/**
 * Base class for admin events.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class RabbitAdminEvent extends AmqpEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	public RabbitAdminEvent(Object source) {
		super(source);
	}

}
