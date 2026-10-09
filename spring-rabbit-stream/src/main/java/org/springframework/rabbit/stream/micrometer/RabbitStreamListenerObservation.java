/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.rabbit.stream.micrometer;

import io.micrometer.common.KeyValues;
import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;

/**
 * Spring Rabbit Observation for stream listeners.
 *
 * @author Gary Russell
 * @since 3.0.5
 *
 */
public enum RabbitStreamListenerObservation implements ObservationDocumentation {

	/**
	 * Observation for Rabbit stream listeners.
	 */
	STREAM_LISTENER_OBSERVATION {


		@Override
		public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
			return DefaultRabbitStreamListenerObservationConvention.class;
		}

		@Override
		public String getPrefix() {
			return "spring.rabbit.stream.listener";
		}

		@Override
		public KeyName[] getLowCardinalityKeyNames() {
			return ListenerLowCardinalityTags.values();
		}

	};

	/**
	 * Low cardinality tags.
	 */
	public enum ListenerLowCardinalityTags implements KeyName {

		/**
		 * Listener id.
		 */
		LISTENER_ID {

			@Override
			public String asString() {
				return "spring.rabbit.stream.listener.id";
			}

		}

	}

	/**
	 * Default {@link RabbitStreamListenerObservationConvention} for Rabbit listener key values.
	 */
	public static class DefaultRabbitStreamListenerObservationConvention
			implements RabbitStreamListenerObservationConvention {

		/**
		 * A singleton instance of the convention.
		 */
		public static final DefaultRabbitStreamListenerObservationConvention INSTANCE =
				new DefaultRabbitStreamListenerObservationConvention();

		@Override
		public KeyValues getLowCardinalityKeyValues(RabbitStreamMessageReceiverContext context) {
			return KeyValues.of(RabbitStreamListenerObservation.ListenerLowCardinalityTags.LISTENER_ID.asString(),
							context.getListenerId());
		}

		@Override
		public String getContextualName(RabbitStreamMessageReceiverContext context) {
			return context.getSource() + " receive";
		}

	}

}
