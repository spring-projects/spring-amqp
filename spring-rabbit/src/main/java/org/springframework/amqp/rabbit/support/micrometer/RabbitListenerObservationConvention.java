/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbit.support.micrometer;

import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;

/**
 * {@link ObservationConvention} for Rabbit listener key values.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public interface RabbitListenerObservationConvention extends ObservationConvention<RabbitMessageReceiverContext> {

	@Override
	default boolean supportsContext(Context context) {
		return context instanceof RabbitMessageReceiverContext;
	}

	@Override
	default String getName() {
		return "spring.rabbit.listener";
	}

}
