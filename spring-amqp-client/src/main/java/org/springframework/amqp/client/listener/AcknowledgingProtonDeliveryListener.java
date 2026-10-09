/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client.listener;

import org.apache.qpid.protonj2.client.Delivery;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.AmqpAcknowledgment;

/**
 * The {@link ProtonDeliveryListener} extension with an {@link AmqpAcknowledgment} callback
 * provided from the container.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
@FunctionalInterface
public interface AcknowledgingProtonDeliveryListener extends ProtonDeliveryListener {

	/**
	 * Process ProtonJ {@link Delivery} and optionally acknowledge it with a callback from the listener container.
	 * The implementation may choose to settle and replenish link credits some other way.
	 * @param delivery the delivery to handle.
	 * @param acknowledgment the acknowledgment callback for this delivery.
	 * @throws Exception any exception from the handling logic.
	 */
	void onDelivery(Delivery delivery, @Nullable AmqpAcknowledgment acknowledgment) throws Exception;

	@Override
	default void onDelivery(Delivery delivery) throws Exception {
		onDelivery(delivery, null);
	}

}
