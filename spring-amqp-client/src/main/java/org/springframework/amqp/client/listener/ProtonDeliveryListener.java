/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client.listener;

import org.apache.qpid.protonj2.client.Delivery;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

/**
 * A message listener extension to process ProtonJ native {@link Delivery} objects.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
@FunctionalInterface
public interface ProtonDeliveryListener extends MessageListener {

	/**
	 * Process ProtonJ {@link Delivery}.
	 * @param delivery the delivery to handle.
	 * @throws Exception any exception from the handling logic.
	 */
	void onDelivery(Delivery delivery) throws Exception;

	@Override
	default void onMessage(Message message) {
		throw new UnsupportedOperationException("The 'onDelivery(Delivery)' has to be called instead.");
	}

}
