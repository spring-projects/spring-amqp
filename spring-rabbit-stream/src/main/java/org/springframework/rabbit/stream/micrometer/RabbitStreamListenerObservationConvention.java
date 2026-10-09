/*
 * Copyright 2023-present the original author or authors.
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
