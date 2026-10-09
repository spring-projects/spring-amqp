/*
 * Copyright 2022-present the original author or authors.
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
