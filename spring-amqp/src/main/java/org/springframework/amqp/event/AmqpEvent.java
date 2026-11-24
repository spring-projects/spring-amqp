/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
