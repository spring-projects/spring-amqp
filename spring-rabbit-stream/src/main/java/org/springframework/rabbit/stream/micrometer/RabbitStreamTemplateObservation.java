/*
 * Copyright 2023-2025 the original author or authors.
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

import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;

/**
 * Spring RabbitMQ Observation for
 * {@link org.springframework.rabbit.stream.producer.RabbitStreamTemplate}.
 *
 * @author Gary Russell
 * @since 3.0.5
 *
 */
public enum RabbitStreamTemplateObservation implements ObservationDocumentation {

	/**
	 * Observation for {@link RabbitStreamTemplate}s.
	 */
	STREAM_TEMPLATE_OBSERVATION {

		@Override
		public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
			return DefaultRabbitStreamTemplateObservationConvention.class;
		}

		@Override
		public String getPrefix() {
			return "spring.rabbit.stream.template";
		}

		@Override
		public KeyName[] getLowCardinalityKeyNames() {
			return TemplateLowCardinalityTags.values();
		}

	};

	/**
	 * Low cardinality tags.
	 */
	public enum TemplateLowCardinalityTags implements KeyName {

		/**
		 * Bean name of the template.
		 */
		BEAN_NAME {

			@Override
			public String asString() {
				return "spring.rabbit.stream.template.name";
			}

		}

	}

	/**
	 * Default {@link RabbitStreamTemplateObservationConvention} for Rabbit template key values.
	 */
	public static class DefaultRabbitStreamTemplateObservationConvention
			implements RabbitStreamTemplateObservationConvention {

		/**
		 * A singleton instance of the convention.
		 */
		public static final DefaultRabbitStreamTemplateObservationConvention INSTANCE =
				new DefaultRabbitStreamTemplateObservationConvention();

		@Override
		public KeyValues getLowCardinalityKeyValues(RabbitStreamMessageSenderContext context) {
			return KeyValues.of(RabbitStreamTemplateObservation.TemplateLowCardinalityTags.BEAN_NAME.asString(),
							context.getBeanName());
		}

		@Override
		public String getContextualName(RabbitStreamMessageSenderContext context) {
			return context.getDestination() + " send";
		}

	}

}
