/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
