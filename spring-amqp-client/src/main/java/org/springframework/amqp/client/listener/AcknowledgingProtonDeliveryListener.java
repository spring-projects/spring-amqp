/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
