/*
 * Copyright 2023 the original author or authors.
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
 * @since 3.0
 *
 */
public enum RabbitStreamListenerObservation implements ObservationDocumentation {

	/**
	 * Observation for Rabbit stream listeners.
	 */
	LISTENER_OBSERVATION {


		@Override
		public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
			return DefaultRabbitStreamListenerObservationConvention.class;
		}

		@Override
		public String getPrefix() {
			return "spring.rabbit.listener";
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
				return "spring.rabbit.listener.id";
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
