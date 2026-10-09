/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpException;

/**
 * Used in several places in the framework, such as
 * {@code AmqpTemplate#convertAndSend(Object, MessagePostProcessor)} where it can be used
 * to add/modify headers or properties after the message conversion has been performed. It
 * also can be used to modify inbound messages when receiving messages in listener
 * containers and {@code AmqpTemplate}s.
 *
 * <p>
 * It is a {@link FunctionalInterface} and is often used as a lambda:
 * <pre class="code">
 * amqpTemplate.convertAndSend(routingKey, m -&gt; {
 *     m.getMessageProperties().setDeliveryMode(DeliveryMode.NON_PERSISTENT);
 *     return m;
 * });
 * </pre>
 *
 * @author Mark Pollack
 * @author Gary Russell
 */
@FunctionalInterface
public interface MessagePostProcessor {

	/**
	 * Change (or replace) the message.
	 * @param message the message.
	 * @return the message.
	 * @throws AmqpException an exception.
	 */
	Message postProcessMessage(Message message) throws AmqpException;

	/**
	 * Change (or replace) the message and/or change its correlation data. Only applies to
	 * outbound messages.
	 * @param message the message.
	 * @param correlation the correlation data.
	 * @return the message.
	 * @since 1.6.7
	 */
	default Message postProcessMessage(Message message, @Nullable Correlation correlation) {
		return postProcessMessage(message);
	}

	/**
	 * Change (or replace) the message and/or change its correlation data. Only applies to
	 * outbound messages.
	 * @param message the message.
	 * @param correlation the correlation data.
	 * @param exchange the exchange to which the message is to be sent.
	 * @param routingKey the routing key.
	 * @return the message.
	 * @since 2.3.4
	 */
	default Message postProcessMessage(Message message, @Nullable Correlation correlation,
			String exchange, String routingKey) {

		return postProcessMessage(message, correlation);
	}

}
