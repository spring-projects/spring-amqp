/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CorrelationData;

/**
 * A callback invoked immediately before publishing a message to update, replace, or
 * create correlation data for publisher confirms. Invoked after conversion and all
 * {@link org.springframework.amqp.core.MessagePostProcessor}s.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6.7
 *
 */
@FunctionalInterface
public interface CorrelationDataPostProcessor {

	/**
	 * Update or replace the correlation data provided in the send method.
	 * @param message the message.
	 * @param correlationData the existing data (if present).
	 * @return the correlation data.
	 */
	CorrelationData postProcess(Message message, @Nullable CorrelationData correlationData);

}
