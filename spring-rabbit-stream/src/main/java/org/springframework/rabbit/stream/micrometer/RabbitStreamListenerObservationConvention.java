/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.rabbit.stream.micrometer;

import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;

/**
 * {@link ObservationConvention} for Rabbit stream listener key values.
 *
 * @author Gary Russell
 * @since 3.0.5
 *
 */
public interface RabbitStreamListenerObservationConvention
		extends ObservationConvention<RabbitStreamMessageReceiverContext> {

	@Override
	default boolean supportsContext(Context context) {
		return context instanceof RabbitStreamMessageReceiverContext;
	}

	@Override
	default String getName() {
		return "spring.rabbit.stream.listener";
	}

}
