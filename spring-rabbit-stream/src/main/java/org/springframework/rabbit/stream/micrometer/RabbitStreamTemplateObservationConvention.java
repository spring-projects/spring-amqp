/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.rabbit.stream.micrometer;

import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;

/**
 * {@link ObservationConvention} for Rabbit stream template key values.
 *
 * @author Gary Russell
 * @since 3.0.5
 *
 */
public interface RabbitStreamTemplateObservationConvention
		extends ObservationConvention<RabbitStreamMessageSenderContext> {

	@Override
	default boolean supportsContext(Context context) {
		return context instanceof RabbitStreamMessageSenderContext;
	}

	@Override
	default String getName() {
		return "spring.rabbit.stream.template";
	}

}
