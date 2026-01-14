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
	 */
	void onDelivery(Delivery delivery);

	@Override
	default void onMessage(Message message) {
		throw new UnsupportedOperationException("The 'onDelivery(Delivery)' has to be called instead.");
	}

}
