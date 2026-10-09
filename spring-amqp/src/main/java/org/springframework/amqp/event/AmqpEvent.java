/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.amqp.event;

import org.springframework.context.ApplicationEvent;


/**
 * Base class for events.
 *
 * @author Gary Russell
 * @since 1.5
 *
 */
@SuppressWarnings("serial")
public abstract class AmqpEvent extends ApplicationEvent {

	public AmqpEvent(Object source) {
		super(source);
	}

}
